"""Yahoo pilot: source-specific staging, explicit production upload, no silent fallback."""
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
        d=Decimal(str(x)); return format(d.normalize(),'f') if d.is_finite() else None
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
        ticker=factory(symbol+'.NS'); currency=None
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
        equations={'BS':('Total Assets','Total Liabilities Net Minority Interest','Total Equity Gross Minority Interest'),'PNL':('Pretax Income','Tax Provision','Net Income Including Noncontrolling Interests'),'CF':('End Cash Position','Beginning Cash Position','Changes In Cash')}
        labels=equations[key[1]]; status='NOT_EVALUATED'; delta=None
        if key[-1]=='INR' and all(len(g.get(x,[]))==1 for x in labels):
            a,b,c=[Decimal(next(iter(g[x]))) for x in labels]; delta=a-b+c if key[1]=='PNL' else a-b-c
            # PBT - tax = net income
            if key[1]=='PNL':delta=a-b-c
            status='PASS' if abs(delta)<=Decimal('1') else 'REVIEW'
        results.append(dict(symbol=key[0],statement=key[1],frequency=key[2],period_end=key[3],status=status,delta=str(delta) if delta is not None else ''))
    return results

def store(db,rows,stamp,sha,summary,remote=False):
    if remote:db.guard()
    for s in SCHEMA:db.run(s)
    cache={}
    def ident(table,**kw):
        k=(table,tuple(kw.items()))
        if k not in cache:cache[k]=db.id(table,**kw)
        return cache[k]
    for i,r in enumerate(rows):
        if remote and i%50==0:db.guard()
        c=ident('dm_yahoo_company',symbol=r['symbol'],ticker=r['symbol']+'.NS'); m=ident('dm_yahoo_metric',statement=r['statement'],label=r['metric']); p=ident('dm_yahoo_period',frequency=r['frequency'],period_end=r['period_end'])
        args=(c,m,p,r['unit'],r['value_raw']); db.run('INSERT OR IGNORE INTO ft_yahoo_financials(company_id,metric_id,period_id,unit,value_raw,first_seen) VALUES(?,?,?,?,?,?)',args+(stamp,))
        fid=db.run('SELECT id FROM ft_yahoo_financials WHERE company_id=? AND metric_id=? AND period_id=? AND unit=? AND value_raw=?',args)[0][0]
        db.run('INSERT OR IGNORE INTO ft_yahoo_source_reference VALUES(?,?,?)',(int(fid),sha,stamp))
    db.run('INSERT OR IGNORE INTO misc_yahoo_runs VALUES(?,?)',(stamp,json.dumps(summary)))

def csvout(path,rows):
    if rows:
        with path.open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    else:path.write_text('')

def main():
    p=argparse.ArgumentParser();p.add_argument('--publish',action='store_true');a=p.parse_args();out=Path('reports/yahoo');out.mkdir(parents=True,exist_ok=True)
    import yfinance as yf
    stamp=datetime.now(timezone.utc).isoformat();rows,coverage,errors=collect(yf.Ticker,out); raw=json.dumps(rows,sort_keys=True).encode();sha=hashlib.sha256(raw).hexdigest();(out/'snapshot.json').write_bytes(raw)
    validation=checks(rows);summary=dict(companies=len({r['symbol'] for r in rows}),facts=len(rows),errors=errors,sha256=sha,expansion_allowed=False,independent_verification=False,production='NOT_ATTEMPTED')
    for name,data in [('observations',rows),('coverage',coverage),('accounting',validation)]:csvout(out/(name+'.csv'),data)
    local=DB.__new__(DB);local.local=sqlite3.connect(out/'pilot.sqlite');local.local.execute('PRAGMA foreign_keys=ON');store(local,rows,stamp,sha,summary);local.local.close()
    try:
        if a.publish:
            if not os.getenv('TURSO_DATABASE_URL') or not os.getenv('TURSO_AUTH_TOKEN'):raise RuntimeError('Missing Turso credentials')
            if summary['companies']!=5 or errors or len(coverage)!=30 or any(not x['facts'] for x in coverage):raise RuntimeError('Incomplete acquisition; publication blocked')
            store(DB(),rows,stamp,sha,summary,True);summary['production']='PUBLISHED_UNVERIFIED'
    except Exception as e:summary['production']='BLOCKED_OR_PARTIAL';summary['publish_error']=str(e);raise
    finally:(out/'summary.json').write_text(json.dumps(summary,indent=2))
    if summary['companies']!=5 or errors or any(not x['facts'] for x in coverage):raise SystemExit(2)
if __name__=='__main__':main()
