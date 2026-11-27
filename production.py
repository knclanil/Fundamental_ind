"""Consolidated candidate: extended acquisition, immutable payloads and explicit gates."""
import argparse, csv, hashlib, json, os, sqlite3, time, zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from database import DB
from yahoo_pilot import WATCH, ROUTES, extract, csvout, store, publishable
from yahoo_quality import classify, validate, freshness, persist
from yahoo_wide import install
from yahoo_deep import inventory, CORE
OUT=Path('reports/production')
ROUTES.update(ttm_income_stmt=('PNL','TTM'),ttm_cashflow=('CF','TTM'))
MODULES={'profile':lambda t:t.get_info(),'prices':lambda t:t.history(period='5y',auto_adjust=False,actions=True),'shares':lambda t:t.get_shares_full(start=str(date.today().replace(year=date.today().year-5,day=1))), 'dividends':lambda t:t.dividends,'splits':lambda t:t.splits,'institutional':lambda t:t.institutional_holders,'major_holders':lambda t:t.major_holders,'mutual_funds':lambda t:t.mutualfund_holders,'earnings_estimates':lambda t:t.earnings_estimate,'revenue_estimates':lambda t:t.revenue_estimate,'growth_estimates':lambda t:t.growth_estimates,'earnings_history':lambda t:t.earnings_history,'earnings_dates':lambda t:t.get_earnings_dates(),'recommendations':lambda t:t.recommendations,'price_targets':lambda t:t.analyst_price_targets,'eps_revisions':lambda t:t.eps_revisions}
def encode(x):
    if hasattr(x,'to_json'):return json.loads(x.to_json(date_format='iso'))
    return x

def acquire(factory, symbols=None):
    rows=[]; coverage=[]; payloads=[]; audit=[]
    for s in (symbols if symbols is not None else WATCH):
        t=factory(s+'.NS'); currency=None
        for name,fn in MODULES.items():
            try:
                time.sleep(float(os.getenv("YAHOO_DELAY", "1.2"))); v=fn(t); data=encode(v)
                if name=='profile':currency=v.get('financialCurrency')
                payloads.append(dict(symbol=s,module=name,data=data)); status='AVAILABLE' if data else 'EMPTY'
            except Exception as e:status=type(e).__name__
            audit.append(dict(symbol=s,module=name,status=status))
        for route in ROUTES:
            try:
                time.sleep(float(os.getenv("YAHOO_DELAY", "1.2"))); batch=extract(getattr(t,route),s,route,currency); rows.extend(batch); status='AVAILABLE' if batch else 'EMPTY'
            except Exception as e:batch=[];status=type(e).__name__
            ends=sorted({r['period_end'] for r in batch});coverage.append(dict(symbol=s,route=route,facts=len(batch),periods=len(ends),earliest=min(ends,default=''),latest=max(ends,default=''),currency=currency or 'UNKNOWN'));audit.append(dict(symbol=s,module=route,status=status))
    return rows,coverage,payloads,audit

def gaps(rows,today=None):
    today=today or date.today(); last=today.year-(today.month<4); seen={(r['symbol'],r['statement'],r['frequency'],r['period_end'],r['metric']) for r in rows}; result=[]
    for s in WATCH:
        for y in range(last-4,last+1):
            for stmt,metrics in CORE.items():
                periods=[('ANNUAL',f'{y}-03-31')]+([('QUARTERLY',f'{y-1}-{m}') for m in ('06-30','09-30','12-31')]+[('QUARTERLY',f'{y}-03-31')] if stmt=='PNL' else [('QUARTERLY',f'{y-1}-09-30')])
                for freq,end in periods:
                    for metric in metrics:
                        key=(s,stmt,freq,end,metric);result.append(dict(zip(('symbol','statement','frequency','period_end','metric'),key),status='AVAILABLE' if key in seen else 'MISSING',expectation='CANDIDATE_NOT_REGULATORY'))
    return result

def payload_store(db,payloads,stamp):
    db.run('CREATE TABLE IF NOT EXISTS ft_yahoo_extended(symbol TEXT,module TEXT,sha TEXT,payload TEXT,first_seen TEXT,PRIMARY KEY(symbol,module,sha))')
    for p in payloads:
        raw=json.dumps(p['data'],sort_keys=True,default=str);sha=hashlib.sha256(raw.encode()).hexdigest();db.run('INSERT OR IGNORE INTO ft_yahoo_extended VALUES(?,?,?,?,?)',(p['symbol'],p['module'],sha,raw,stamp))
    db.run('CREATE VIEW IF NOT EXISTS vw_fund_yahoo_extended AS SELECT * FROM ft_yahoo_extended')

def run(db,rows,coverage,payloads,stamp,sha,remote=False):
    if remote and os.getenv('YAHOO_IGNORE_STORAGE_BUDGET')!='true':db.guard()
    if remote and os.getenv('YAHOO_IGNORE_STORAGE_BUDGET')=='true' and not {r['symbol'] for r in rows} <= set(WATCH):raise RuntimeError('Storage override restricted to pilot')
    classified=classify(rows,coverage); checks=validate(classified); fresh=freshness(classified); summary={}
    db.run('CREATE TABLE IF NOT EXISTS misc_yahoo_publication(retrieved TEXT PRIMARY KEY,sha TEXT,status TEXT)')
    db.run('INSERT OR IGNORE INTO misc_yahoo_publication VALUES(?,?,?)',(stamp,sha,'STAGING'))
    store(db,rows,stamp,sha,summary,remote);persist(db,classified,checks,fresh,stamp,sha);payload_store(db,payloads,stamp);install(db, count_views=not remote)
    db.run("UPDATE misc_yahoo_publication SET status='COMPLETE' WHERE retrieved=? AND sha=?",(stamp,sha))
    return classified,checks,fresh

def main():
    p=argparse.ArgumentParser();p.add_argument('--replay',type=Path);p.add_argument('--publish',action='store_true');a=p.parse_args();OUT.mkdir(parents=True,exist_ok=True);stamp=datetime.now(timezone.utc).isoformat()
    if a.replay:
        with zipfile.ZipFile(a.replay) as z:
            raw=z.read('snapshot.json');meta=json.loads(z.read('summary.json'))
            if hashlib.sha256(raw).hexdigest()!=meta['sha256']:raise ValueError('checksum mismatch')
            rows=json.loads(raw);coverage=list(csv.DictReader(z.read('coverage.csv').decode().splitlines()));payloads=[dict(symbol=x['symbol'],module='profile',data=x['profile']) for x in json.loads(z.read('company_profiles.json'))];audit=[]
            from yahoo_pilot import unit
            for r in csv.DictReader(z.read('trailing_financials.csv').decode().splitlines()):
                c=r.pop('currency');r['unit']=unit(r['metric'],c);rows.append(r)
    else:
        import yfinance as yf
        rows,coverage,payloads,audit=acquire(yf.Ticker)
    rows=list({tuple(sorted(r.items())):r for r in rows}.values());sha=hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest();db=DB.__new__(DB);db.local=sqlite3.connect(OUT/'candidate.sqlite');db.local.execute('PRAGMA foreign_keys=ON');classified,checks,fresh=run(db,rows,coverage,payloads,stamp,sha);g=gaps(rows)
    for name,data in [('observations',rows),('classified',classified),('accounting',checks),('freshness',fresh),('coverage',coverage),('expected_cells',g),('data_gaps',[r for r in g if r['status']=='MISSING']),('canonical_inventory',inventory(rows)),('source_audit',audit)]:csvout(OUT/(name+'.csv'),data)
    for name in ('pnl_wide','bs_wide','cf_wide','financial_summary','ratios','growth'):
        cur=db.local.execute('SELECT * FROM vw_fund_yahoo_'+name);csvout(OUT/(name+'.csv'),[dict(zip([d[0] for d in cur.description],r)) for r in cur])
    summary=dict(facts=len(rows),ttm=sum(r['frequency']=='TTM' for r in rows),payloads=len(payloads),missing=sum(r['status']=='MISSING' for r in g),integrity=db.local.execute('PRAGMA integrity_check').fetchone()[0],production='NOT_ATTEMPTED',expansion_allowed=False,blockers=['independent verification','historical gaps','source permissions','live storage and recovery validation','legacy ownership and backup validation']);db.local.close()
    try:
        if a.publish:
            if a.replay or not os.getenv('TURSO_DATABASE_URL') or not os.getenv('TURSO_AUTH_TOKEN') or not publishable(rows):raise RuntimeError('Publication prerequisites missing')
            remote=DB();summary['storage_before_bytes']=remote.storage();summary['storage_override']=os.getenv('YAHOO_IGNORE_STORAGE_BUDGET')=='true'
            summary['production']='PUBLISHING'
            run(remote,rows,coverage,payloads,stamp,sha,True);summary['production']='PUBLISHED_UNVERIFIED'
            summary['storage_after_bytes']=remote.storage()
    except Exception as exc:
        summary['production']='PUBLICATION_FAILED';summary['error_type']=type(exc).__name__
        summary['error_reason']='STORAGE_GUARD' if str(exc).startswith('Storage guard:') else 'PUBLICATION_ERROR'
        raise
    finally:(OUT/'production_readiness.json').write_text(json.dumps(summary,indent=2))
if __name__=='__main__':main()
