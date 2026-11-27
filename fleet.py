"""Resume Nifty 500 ingestion across scheduled runs. Missing data never becomes zero."""
import argparse,hashlib,json,os,time,traceback
from datetime import datetime,timezone,timedelta
from pathlib import Path
from database import DB
from universe import fetch
from production import acquire,run
from yahoo_wide import install
from yahoo_pilot import csvout
from analytics import install_extra

def now():return datetime.now(timezone.utc).isoformat()
def main():
    p=argparse.ArgumentParser();p.add_argument('--limit',type=int,default=500);p.add_argument('--minutes',type=int,default=300);a=p.parse_args()
    deadline=time.monotonic()+a.minutes*60
    if not os.getenv('TURSO_DATABASE_URL') or not os.getenv('TURSO_AUTH_TOKEN'):raise RuntimeError('Remote credentials required')
    os.environ.pop('YAHOO_IGNORE_STORAGE_BUDGET',None)
    import yfinance as yf
    db=DB();out=Path('reports/fleet');out.mkdir(parents=True,exist_ok=True)
    db.run('CREATE TABLE IF NOT EXISTS dm_yahoo_universe(symbol TEXT PRIMARY KEY,isin TEXT,industry TEXT,company_name TEXT,active INTEGER,last_seen TEXT)')
    db.run('CREATE TABLE IF NOT EXISTS misc_yahoo_queue(symbol TEXT PRIMARY KEY,last_attempt TEXT,last_success TEXT,status TEXT,error TEXT)')
    members=fetch();stamp=now()
    for s,(isin,industry,name) in members.items():
        db.run('INSERT INTO dm_yahoo_universe VALUES(?,?,?,?,1,?) ON CONFLICT(symbol) DO UPDATE SET isin=excluded.isin,industry=excluded.industry,company_name=excluded.company_name,active=1,last_seen=excluded.last_seen',(s,isin,industry,name,stamp))
        db.run('INSERT OR IGNORE INTO misc_yahoo_queue(symbol,status) VALUES(?,?)',(s,'PENDING'))
    db.run('UPDATE dm_yahoo_universe SET active=0 WHERE last_seen<>?',(stamp,))
    cutoff=(datetime.now(timezone.utc)-timedelta(days=7)).isoformat()
    selected=db.run('SELECT q.symbol FROM misc_yahoo_queue q JOIN dm_yahoo_universe u USING(symbol) WHERE u.active=1 AND (q.last_success IS NULL OR q.last_success<?) ORDER BY COALESCE(q.last_attempt,\'\'),q.symbol LIMIT ?',(cutoff,a.limit))
    audit=[]; failures=0
    for (s,) in selected:
        if time.monotonic()>=deadline:break
        db.guard(); stamp=now();db.run("UPDATE misc_yahoo_queue SET last_attempt=?,status='RUNNING' WHERE symbol=?",(stamp,s))
        try:
            rows,coverage,payloads,checks=acquire(yf.Ticker,[s]);audit.extend(checks)
            rows=list({tuple(sorted(r.items())):r for r in rows}.values())
            if not rows:raise RuntimeError('No financial observations')
            sha=hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest()
            run(db,rows,coverage,payloads,stamp,sha,True)
            complete={r['statement'] for r in rows if r['frequency']=='ANNUAL'}=={'PNL','BS','CF'}
            db.run('UPDATE misc_yahoo_queue SET last_success=?,status=?,error=NULL WHERE symbol=?',(stamp if complete else None,'PUBLISHED_UNVERIFIED' if complete else 'PARTIAL',s))
        except Exception as e:
            failures+=1;db.run("UPDATE misc_yahoo_queue SET status='FAILED',error=? WHERE symbol=?",(type(e).__name__,s))
            if failures>=3:break
    if db.run("SELECT name FROM sqlite_schema WHERE name='vw_fund_yahoo_financial_summary'"):install_extra(db)
    csvout(out/'source_audit.csv',audit)
    report={'universe':len(members),'queue':db.run('SELECT status,count(*) FROM misc_yahoo_queue JOIN dm_yahoo_universe USING(symbol) WHERE active=1 GROUP BY status'),'allocated_bytes':db.storage(),'occupied_bytes':db.occupied(),'independently_verified':False,'failures':failures,'selected':len(selected),'time_budget_reached':time.monotonic()>=deadline}
    (out/'summary.json').write_text(json.dumps(report,indent=2));print(report)
    if failures:raise RuntimeError('One or more companies failed; queue retained')
if __name__=='__main__':main()
