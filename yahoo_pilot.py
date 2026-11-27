"""Yahoo pilot: source-specific staging, explicit production upload, no silent fallback."""
from tickers import ticker_for
import argparse, csv, json, os, sqlite3, time, hashlib
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, date
from database import DB

WATCH=json.loads(Path(__file__).with_name('watchlist.json').read_text())
ROUTES={'income_stmt':('PNL','ANNUAL'),'quarterly_income_stmt':('PNL','QUARTERLY'),'balance_sheet':('BS','ANNUAL'),'quarterly_balance_sheet':('BS','QUARTERLY'),'cashflow':('CF','ANNUAL'),'quarterly_cashflow':('CF','QUARTERLY')}
SCHEMA=[
'CREATE TABLE IF NOT EXISTS dm_yahoo_company(id INTEGER PRIMARY KEY,symbol TEXT UNIQUE NOT NULL,ticker TEXT NOT NULL)',
'CREATE TABLE IF NOT EXISTS dm_yahoo_metric(id INTEGER PRIMARY KEY,statement TEXT NOT NULL,label TEXT NOT NULL,UNIQUE(statement,label))',
'CREATE TABLE IF NOT EXISTS dm_yahoo_period(id INTEGER PRIMARY KEY,frequency TEXT NOT NULL,period_end TEXT NOT NULL,UNIQUE(frequency,period_end))',
'CREATE TABLE IF NOT EXISTS ft_yahoo_financials(id INTEGER PRIMARY KEY,company_id INTEGER NOT NULL REFERENCES dm_yahoo_company(id),metric_id INTEGER NOT NULL REFERENCES dm_yahoo_metric(id),period_id INTEGER NOT NULL REFERENCES dm_yahoo_period(id),unit TEXT NOT NULL,value_raw TEXT NOT NULL,first_seen TEXT NOT NULL,UNIQUE(company_id,metric_id,period_id,unit,value_raw))',
'CREATE TABLE IF NOT EXISTS ft_yahoo_source_reference(fact_id INTEGER NOT NULL REFERENCES ft_yahoo_financials(id),snapshot_sha TEXT NOT NULL,retrieved TEXT NOT NULL,PRIMARY KEY(fact_id,snapshot_sha,retrieved))',
'CREATE TABLE IF NOT EXISTS misc_yahoo_runs(run_id TEXT PRIMARY KEY,summary TEXT NOT NULL)',
'''CREATE VIEW IF NOT EXISTS vw_fund_yahoo_observations AS SELECT c.symbol,c.ticker,m.statement,m.label metric,p.frequency,p.period_end,f.unit,f.value_raw,CASE WHEN f.unit='INR' THEN CAST(f.value_raw AS REAL)/10000000.0 END value_crores,f.first_seen,'UNKNOWN' consolidation,'UNVERIFIED' quality_status FROM ft_yahoo_financials f JOIN dm_yahoo_company c ON c.id=f.company_id JOIN dm_yahoo_metric m ON m.id=f.metric_id JOIN dm_yahoo_period p ON p.id=f.period_id''']
for name,statement in [('pnl','PNL'),('balance_sheet','BS'),('cash_flow','CF')]:
    SCHEMA.append(f"CREATE VIEW IF NOT EXISTS vw_fund_yahoo_{name} AS SELECT * FROM vw_fund_yahoo_observations WHERE statement='{statement}'")
# Explicit conservative unit mapping: unknown labels are retained without scaling.
MONEY=set('Total Revenue|Operating Revenue|Cost Of Revenue|Gross Profit|Operating Income|Pretax Income|Tax Provision|Net Income|Net Income Common Stockholders|Net Income Including Noncontrolling Interests|Total Assets|Total Liabilities Net Minority Interest|Total Equity Gross Minority Interest|Stockholders Equity|Operating Cash Flow|Investing Cash Flow|Financing Cash Flow|Capital Expenditure|Changes In Cash|Beginning Cash Position|End Cash Position|Interest Income|Interest Expense|Net Interest Income|Total Debt|Cash And Cash Equivalents|Inventory|Receivables|Total Expenses|EBITDA|EBIT'.split('|'))
def unit(label,currency):
    if label in MONEY:return currency or 'UNKNOWN'
    if label in ('Basic EPS','Diluted EPS'):return (currency+'/share') if currency else 'UNKNOWN'
    if 'Shares' in label or label in ('Basic Average Shares','Diluted Average Shares'):return 'SHARES'
    return 'UNKNOWN'
def number(x):
    try:
        d=Decimal(str(x)); return (format(d,'f').rstrip('0').rstrip('.') if '.' in format(d,'f') else format(d,'f')) if d.is_finite() else None
    except (InvalidOperation,ValueError):return None

def extract(frame,symbol,route,currency):
    statement,freq=ROUTES[route]; rows=[]
    for i,label in enumerate(frame.index):
        for j,col in enumerate(frame.columns):
            try:end=date.fromisoformat(str(col)[:10]).isoformat()
            except ValueError:continue
            value=number(frame.iloc[i,j])
            if value is not None:rows.append(dict(symbol=symbol,statement=statement,frequency=freq,period_end=end,metric=str(label),unit=unit(str(label),currency),value_raw=value))
    return rows

def collect(factory,out):
    rows=[]; coverage=[]; errors=[]
    for symbol in WATCH:
        ticker=factory(ticker_for(symbol)); currency=None
        try:currency=ticker.get_info().get('financialCurrency')
        except Exception as e:errors.append(dict(symbol=symbol,route='profile',error=type(e).__name__))
        for route in ROUTES:
            try:
                time.sleep(float(os.getenv('YAHOO_DELAY','1.2')))
                batch=extract(getattr(ticker,route),symbol,route,currency); rows.extend(batch)
                periods=sorted({r['period_end'] for r in batch}); coverage.append(dict(symbol=symbol,route=route,facts=len(batch),periods=len(periods),earliest=min(periods,default=''),latest=max(periods,default=''),currency=currency or 'UNKNOWN'))
            except Exception as e:errors.append(dict(symbol=symbol,route=route,error=type(e).__name__))
    return rows,coverage,errors

def checks(rows):
    groups={}
    for r in rows:groups.setdefault((r['symbol'],r['statement'],r['frequency'],r['period_end'],r['unit']),{}).setdefault(r['metric'],set()).add(r['value_raw'])
    results=[]
    for key,g in groups.items():
        if key[-1] not in ('INR','USD','UNKNOWN'):continue
        if key[-1]=='UNKNOWN' and any(k[:4]==key[:4] and k[-1] in ('INR','USD') for k in groups):continue
        equations={'BS':('Total Assets','Total Liabilities Net Minority Interest','Total Equity Gross Minority Interest'),'PNL':('Pretax Income','Tax Provision','Net Income Including Noncontrolling Interests'),'CF':('End Cash Position','Beginning Cash Position','Changes In Cash')}
        labels=equations[key[1]]; status='NOT_EVALUATED'; delta=None
        if key[-1] in ('INR','USD') and all(len(g.get(x,[]))==1 for x in labels):
            a,b,c=[Decimal(next(iter(g[x]))) for x in labels]; delta=a-b+c if key[1]=='PNL' else a-b-c
            # PBT - tax = net income
            if key[1]=='PNL':delta=a-b-c
            status='PASS' if abs(delta)<=max(Decimal('1'),max(abs(a),abs(b),abs(c))*Decimal('0.000001')) else 'REVIEW'
        results.append(dict(symbol=key[0],statement=key[1],frequency=key[2],period_end=key[3],status=status,delta=str(delta) if delta is not None else ''))
    return results

def store(db,rows,stamp,sha,summary,remote=False):
    if remote and os.getenv('YAHOO_IGNORE_STORAGE_BUDGET')!='true':db.guard()
    for s in SCHEMA:db.run(s)
    cache={}
    def ident(table,**kw):
        k=(table,tuple(kw.items()))
        if k not in cache:cache[k]=db.id(table,**kw)
        return cache[k]
    if remote:
        # Resolve dimensions once per batch using SQL joins, not per-fact HTTP calls.
        for start in range(0,len(rows),100):
            batch=rows[start:start+100]
            values=','.join(['(?,?,?,?,?,?,?,?)']*len(batch))
            args=tuple(v for r in batch for v in (r['symbol'],ticker_for(r['symbol']),r['statement'],r['metric'],r['frequency'],r['period_end'],r['unit'],r['value_raw']))
            prefix='WITH incoming(symbol,ticker,statement,label,frequency,period_end,unit,value_raw) AS (VALUES '+values+') '
            for table,cols in [('dm_yahoo_company','symbol,ticker'),('dm_yahoo_metric','statement,label'),('dm_yahoo_period','frequency,period_end')]:
                db.run(prefix+f'INSERT OR IGNORE INTO {table} ({cols}) SELECT DISTINCT {cols} FROM incoming',args)
            joins=' FROM incoming i JOIN dm_yahoo_company c ON c.symbol=i.symbol JOIN dm_yahoo_metric m ON m.statement=i.statement AND m.label=i.label JOIN dm_yahoo_period p ON p.frequency=i.frequency AND p.period_end=i.period_end'
            db.run(prefix+'INSERT OR IGNORE INTO ft_yahoo_financials(company_id,metric_id,period_id,unit,value_raw,first_seen) SELECT c.id,m.id,p.id,i.unit,i.value_raw,?'+joins,args+(stamp,))
            db.run(prefix+'INSERT OR IGNORE INTO ft_yahoo_source_reference SELECT f.id,?,?'+joins+' JOIN ft_yahoo_financials f ON f.company_id=c.id AND f.metric_id=m.id AND f.period_id=p.id AND f.unit=i.unit AND f.value_raw=i.value_raw',args+(sha,stamp))
        count=int(db.run('SELECT count(*) FROM ft_yahoo_source_reference WHERE snapshot_sha=? AND retrieved=?',(sha,stamp))[0][0])
        expected=len({tuple(r[k] for k in ('symbol','statement','metric','frequency','period_end','unit','value_raw')) for r in rows})
        if count!=expected:raise RuntimeError(f'Publication count mismatch: {count}/{expected}')
        summary['published_unique_facts']=count
        db.run('INSERT OR REPLACE INTO misc_yahoo_runs VALUES(?,?)',(stamp,json.dumps(dict(summary,production='STAGING'))))
        return
    for i,r in enumerate(rows):
        if remote and i%50==0 and os.getenv('YAHOO_IGNORE_STORAGE_BUDGET')!='true':db.guard()
        db.run('UPDATE dm_yahoo_company SET ticker=? WHERE symbol=?',(ticker_for(r['symbol']),r['symbol']))
        c=ident('dm_yahoo_company',symbol=r['symbol'],ticker=ticker_for(r['symbol'])); m=ident('dm_yahoo_metric',statement=r['statement'],label=r['metric']); p=ident('dm_yahoo_period',frequency=r['frequency'],period_end=r['period_end'])
        args=(c,m,p,r['unit'],r['value_raw']); db.run('INSERT OR IGNORE INTO ft_yahoo_financials(company_id,metric_id,period_id,unit,value_raw,first_seen) VALUES(?,?,?,?,?,?)',args+(stamp,))
        fid=db.run('SELECT id FROM ft_yahoo_financials WHERE company_id=? AND metric_id=? AND period_id=? AND unit=? AND value_raw=?',args)[0][0]
        db.run('INSERT OR IGNORE INTO ft_yahoo_source_reference VALUES(?,?,?)',(int(fid),sha,stamp))
    db.run('INSERT OR IGNORE INTO misc_yahoo_runs VALUES(?,?)',(stamp,json.dumps(summary)))

def csvout(path,rows):
    if rows:
        with path.open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    else:path.write_text('')

def publishable(rows):
    return {(r['symbol'],r['statement']) for r in rows if r['frequency']=='ANNUAL'} >= {(s,t) for s in WATCH for t in ('PNL','BS','CF')}

def main():
    p=argparse.ArgumentParser();p.add_argument('--publish',action='store_true');p.add_argument('--replay',type=Path);a=p.parse_args();out=Path('reports/yahoo');out.mkdir(parents=True,exist_ok=True)
    stamp=datetime.now(timezone.utc).isoformat()
    if a.replay:
        import zipfile
        with zipfile.ZipFile(a.replay) as z:
            original=z.read('snapshot.json'); meta=json.loads(z.read('summary.json')); assert hashlib.sha256(original).hexdigest()==meta['sha256'], 'Snapshot checksum mismatch'
            rows=json.loads(original);coverage=list(csv.DictReader(z.read('coverage.csv').decode().splitlines()));errors=meta['errors']
        for c in coverage:c['facts']=int(c['facts']);c['periods']=int(c['periods'])
    else:
        import yfinance as yf
        rows,coverage,errors=collect(yf.Ticker,out)
    raw=json.dumps(rows,sort_keys=True).encode();sha=hashlib.sha256(raw).hexdigest();(out/'snapshot.json').write_bytes(raw)
    from yahoo_quality import classify,validate,freshness,persist
    classified=classify(rows,coverage); validation=validate(classified); fresh=freshness(classified)
    summary=dict(companies=len({r['symbol'] for r in rows}),facts=len(rows),errors=errors,sha256=sha,expansion_allowed=False,independent_verification=False,production='NOT_ATTEMPTED',coverage_gaps=[c for c in coverage if not c['facts']],currency_basis='Provider metadata; unverified',storage_budget_override=os.getenv('YAHOO_IGNORE_STORAGE_BUDGET')=='true',replayed=bool(a.replay))
    summary['classified_facts']=sum(r['classified_unit']!='UNKNOWN' for r in classified);summary['quality_gate']='BLOCKED_UNVERIFIED'
    for name,data in [('classified',classified),('freshness',fresh),('observations',rows),('coverage',coverage),('accounting',validation)]:csvout(out/(name+'.csv'),data)
    local=DB.__new__(DB);local.local=sqlite3.connect(out/'pilot.sqlite');local.local.execute('PRAGMA foreign_keys=ON');store(local,rows,stamp,sha,summary);persist(local,classified,validation,fresh,stamp,sha);
    from yahoo_wide import install
    summary["wide_views"]=install(local)
    for name in ("pnl_wide","bs_wide","cf_wide","financial_summary","completeness"):
        cursor=local.local.execute("SELECT * FROM vw_fund_yahoo_"+name+" ORDER BY symbol,frequency,period_end DESC")
        csvout(out/(name+".csv"),[dict(zip([d[0] for d in cursor.description],r)) for r in cursor.fetchall()])
    local.local.close()
    try:
        if a.publish:
            if a.replay:raise RuntimeError('Replay is local only; original retrieval timestamp unavailable')
            if not os.getenv('TURSO_DATABASE_URL') or not os.getenv('TURSO_AUTH_TOKEN'):raise RuntimeError('Missing Turso credentials')
            if not publishable(rows):raise RuntimeError('Missing annual statement for a pilot company; publication blocked')
            remote=DB();store(remote,rows,stamp,sha,summary,True);summary['published_classified_facts']=persist(remote,classified,validation,fresh,stamp,sha);summary['remote_wide_views']=install(remote);summary['production']='PUBLISHED_UNVERIFIED'
    except Exception as e:summary['production']='BLOCKED_OR_PARTIAL';summary['publish_error']=str(e);raise
    finally:(out/'summary.json').write_text(json.dumps(summary,indent=2))
    if not publishable(rows):raise SystemExit(2)
if __name__=='__main__':main()
