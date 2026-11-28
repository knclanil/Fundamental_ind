import argparse,os,time,json,hashlib,gzip,traceback
from datetime import datetime,timezone,timedelta
from pathlib import Path
from database import DB
from universe import refresh
from tickers import ticker_for
from yahoo_pilot import extract,ROUTES
from yahoo_quality import classify,validate,freshness,MAPPING
ROUTES.update(ttm_income_stmt=('PNL','TTM'),ttm_cashflow=('CF','TTM'))
def now():return datetime.now(timezone.utc).isoformat()
def raw(x):return json.dumps(x,sort_keys=True,default=str,separators=(',',':')).encode()
def diagnostic(name,e):
 message=f'{name}: {type(e).__name__}: {e}'
 detail=traceback.format_exc()
 for key in ('TURSO_AUTH_TOKEN','TURSO_DATABASE_URL'):
  secret=os.getenv(key)
  if secret:message=message.replace(secret,'[REDACTED]');detail=detail.replace(secret,'[REDACTED]')
 print(message+'\n'+detail,flush=True)
 return message

def normalize_extended(v):
 if v is None or getattr(v,'empty',False):return None
 # Records preserve duplicate dates/columns without requiring unique index keys.
 if hasattr(v,'to_json'):
  if getattr(v,'ndim',None)==1:v=v.to_frame(name='value')
  v=json.loads(v.to_json(orient='split',date_format='iso'))
 if isinstance(v,(dict,list,tuple)) and not v:return None
 return json.loads(json.dumps(v,sort_keys=True,default=str,allow_nan=False))
def seed(db,members):
 existing={r[0]:tuple(r[1:]) for r in db.run('SELECT c.symbol,c.isin,c.industry,c.name,t.ticker FROM dm_company c JOIN dm_ticker t ON t.company_id=c.id WHERE c.active=1')}
 expected={s:(isin,industry,name,ticker_for(s)) for s,(isin,industry,name) in members.items()}
 if existing==expected and int(db.run('SELECT count(*) FROM misc_queue q JOIN dm_company c ON c.id=q.company_id WHERE c.active=1 AND q.dataset_group IN (\'CORE\',\'EXTENDED\')')[0][0])==2*len(members):return
 for s,(isin,industry,name) in members.items():
  db.run('INSERT INTO dm_company(isin,symbol,name,industry) VALUES(?,?,?,?) ON CONFLICT(isin) DO UPDATE SET symbol=excluded.symbol,name=excluded.name,industry=excluded.industry,active=1',(isin,s,name,industry))
  db.run('INSERT INTO dm_ticker SELECT id,? FROM dm_company WHERE isin=? ON CONFLICT(company_id) DO UPDATE SET ticker=excluded.ticker',(ticker_for(s),isin))
  db.run("INSERT OR IGNORE INTO misc_queue(company_id,dataset_group) SELECT id,'CORE' FROM dm_company WHERE isin=?",(isin,))
  db.run("INSERT OR IGNORE INTO misc_queue(company_id,dataset_group) SELECT id,'EXTENDED' FROM dm_company WHERE isin=?",(isin,))
 marks=','.join('?' for _ in members);db.run(f'UPDATE dm_company SET active=0 WHERE isin NOT IN ({marks})',tuple(v[0] for v in members.values()))
MODULES = ("prices","shares","dividends","splits","institutional","major_holders","mutual_funds","earnings_estimates","revenue_estimates","recommendations")
REQUIRED = {"prices", "shares"}
def acquire(factory,s,group,modules=None):
 t=factory(ticker_for(s));rows=[];coverage=[];payload={};errors=[]
 def call(name,fn):
  time.sleep(float(os.getenv('YAHOO_DELAY','1.2')))
  try:return fn()
  except Exception as e:errors.append(diagnostic(name,e));return None
 if group=='CORE':
  info=call('profile',t.get_info) or {};currency=info.get('financialCurrency');payload['profile']=info
  for route in ROUTES:
   frame=call(route,lambda:getattr(t,route));batch=extract(frame,s,route,currency) if frame is not None else [];rows.extend(batch);coverage.append({'symbol':s,'currency':currency,'route':route,'facts':len(batch)})
 else:
  for name,fn in {'prices':lambda:t.history(period='5y',auto_adjust=False,actions=True),'shares':lambda:t.get_shares_full(),'dividends':lambda:t.dividends,'splits':lambda:t.splits,'institutional':lambda:t.institutional_holders,'major_holders':lambda:t.major_holders,'mutual_funds':lambda:t.mutualfund_holders,'earnings_estimates':lambda:t.earnings_estimate,'revenue_estimates':lambda:t.revenue_estimate,'recommendations':lambda:t.recommendations}.items():
   if modules is not None and name not in modules:continue
   v=call(name,lambda:normalize_extended(fn()))
   if v is not None:payload[name]=v
   elif not any(e.startswith(name+':') for e in errors):errors.append(name+': EMPTY_OR_UNAVAILABLE')
   coverage.append({'module':name,'status':'AVAILABLE' if v is not None else 'NOT_AVAILABLE' if errors[-1].endswith(': EMPTY_OR_UNAVAILABLE') and name not in REQUIRED else 'RETRY_PENDING'})
 return rows,coverage,payload,errors
def publish(db,cid,s,rows,coverage,payload,errors):
 classified=classify(rows,coverage); items=sorted({(r['statement'],r['metric'],r['unit_kind'],r['frequency'],r['period_end'],r['classified_unit'],r['value_raw']) for r in classified});stamp=now();sha=hashlib.sha256(raw([items,payload])).digest()
 sid=int(db.run('INSERT INTO dm_snapshot(company_id,retrieved,sha,ticker) VALUES(?,?,?,?) RETURNING id',(cid,stamp,sha,ticker_for(s)))[0][0])
 for start in range(0,len(items),100):
  batch=items[start:start+100];args=tuple(v for r in batch for v in r);prefix='WITH incoming(statement,label,kind,frequency,period_end,unit,value_text) AS (VALUES '+','.join(['(?,?,?,?,?,?,?)']*len(batch))+') '
  for table,cols,select in [('dm_metric','statement,label,kind','statement,label,kind'),('dm_period','frequency,period_end','frequency,period_end'),('dm_unit','code','unit')]:db.run(prefix+f'INSERT OR IGNORE INTO {table}({cols}) SELECT DISTINCT {select} FROM incoming',args)
  joins=' FROM incoming i JOIN dm_metric m ON m.statement=i.statement AND m.label=i.label AND m.kind=i.kind JOIN dm_period p ON p.frequency=i.frequency AND p.period_end=i.period_end JOIN dm_unit u ON u.code=i.unit'
  db.run(prefix+'INSERT OR IGNORE INTO ft_financials(company_id,metric_id,period_id,unit_id,value_text) SELECT ?,m.id,p.id,u.id,i.value_text'+joins,args+(cid,))
  db.run(prefix+'INSERT OR IGNORE INTO ft_source_reference SELECT f.id,?'+joins+' JOIN ft_financials f ON f.company_id=? AND f.metric_id=m.id AND f.period_id=p.id AND f.unit_id=u.id AND f.value_text=i.value_text',args+(sid,cid))
 if int(db.run('SELECT count(*) FROM ft_source_reference WHERE snapshot_id=?',(sid,))[0][0])!=len(items):raise RuntimeError('Fact count mismatch')
 for module,data in payload.items():
  b=raw(data);h=hashlib.sha256(b).digest();db.run('INSERT OR IGNORE INTO dm_payload(sha,payload,original_bytes) VALUES(?,?,?)',(h,gzip.compress(b,mtime=0),len(b)));db.run('INSERT INTO ft_extended SELECT ?,?,id FROM dm_payload WHERE sha=?',(sid,module,h))
 db.run('INSERT INTO misc_quality VALUES(?,?)',(sid,json.dumps({'checks':validate(classified),'freshness':freshness(classified),'coverage':coverage,'errors':errors})))
 db.run("UPDATE dm_snapshot SET status='COMPLETE' WHERE id=?",(sid,))
def install_wide(db):
 import re
 for stmt,short in [('PNL','pnl'),('BS','bs'),('CF','cf')]:
  labels=sorted(set(MAPPING[stmt])|{r[0] for r in db.run('SELECT label FROM dm_metric WHERE statement=?',(stmt,))});used=set();cols=[]
  for label in labels:
   base=re.sub('[^a-z0-9]+','_',label.lower()).strip('_') or 'metric';name=base;n=2
   while name in used:name=base+'_'+str(n);n+=1
   used.add(name);cols.append("MAX(CASE WHEN metric='"+label.replace("'","''")+"' THEN display_value END) AS \""+name+'\"')
  db.run('DROP VIEW IF EXISTS vw_fund_'+short+'_wide');db.run('CREATE VIEW vw_fund_'+short+'_wide AS SELECT symbol,frequency,period_end,'+','.join(cols)+" FROM vw_fund_cells WHERE statement='"+stmt+"' GROUP BY symbol,frequency,period_end")
 db.run('CREATE VIEW IF NOT EXISTS vw_fund_ratios AS SELECT p.symbol,p.frequency,p.period_end,CASE WHEN r.unit=n.unit AND r.unit IN (\'INR\',\'USD\') AND p.total_revenue>0 THEN 100.0*p.net_income/p.total_revenue END net_margin_pct FROM vw_fund_pnl_wide p JOIN vw_fund_cells r ON r.symbol=p.symbol AND r.frequency=p.frequency AND r.period_end=p.period_end AND r.statement=\'PNL\' AND r.metric=\'Total Revenue\' JOIN vw_fund_cells n ON n.symbol=p.symbol AND n.frequency=p.frequency AND n.period_end=p.period_end AND n.statement=\'PNL\' AND n.metric=\'Net Income\'')
def extended_plan(db,cid):
 # Bootstrap only from published snapshots, including 6.0.2 evidence.
 db.run("INSERT OR IGNORE INTO misc_module(company_id,module,status,next_retry) SELECT ?,e.module,'AVAILABLE',strftime('%Y-%m-%dT%H:%M:%f+00:00',MAX(s.retrieved),'+7 days') FROM ft_extended e JOIN dm_snapshot s ON s.id=e.snapshot_id WHERE s.company_id=? AND s.status='COMPLETE' AND e.module<>'profile' GROUP BY e.module",(cid,cid))
 for module in MODULES:
  db.run("INSERT OR IGNORE INTO misc_module(company_id,module,status) VALUES(?,?,'PENDING')",(cid,module))
 return [r[0] for r in db.run("SELECT module FROM misc_module WHERE company_id=? AND (next_retry IS NULL OR julianday(next_retry)<=julianday(?))",(cid,now()))]
def record_modules(db,cid,cov):
 for item in cov:
  status=item['status'];delay=timedelta(hours=6) if status=='RETRY_PENDING' else timedelta(days=7 if status=='AVAILABLE' else 30)
  db.run('UPDATE misc_module SET status=?,next_retry=?,last_attempt=? WHERE company_id=? AND module=?',(status,(datetime.now(timezone.utc)+delay).isoformat(),now(),cid,item['module']))
def extended_status(db,cid):
 states=dict(db.run('SELECT module,status FROM misc_module WHERE company_id=?',(cid,)))
 return 'PUBLISHED_UNVERIFIED' if all(states.get(m)=='AVAILABLE' for m in REQUIRED) and not any(v in ('PENDING','RETRY_PENDING') for v in states.values()) else 'PARTIAL'
def checkpoint(db,out,failures):
 report={'queue':db.run('SELECT * FROM vw_fund_acquisition_progress'),'modules':db.run('SELECT * FROM vw_fund_module_progress'),'occupied_bytes':db.occupied(),'failures':failures}
 temp=out/'summary.tmp';temp.write_text(json.dumps(report,indent=2));temp.replace(out/'summary.json');return report
def main():
 p=argparse.ArgumentParser();p.add_argument('--limit',type=int,default=50);p.add_argument('--minutes',type=int,default=40);p.add_argument('--group',choices=['CORE','EXTENDED'],default='CORE');p.add_argument('--init-only',action='store_true');a=p.parse_args();deadline=time.monotonic()+a.minutes*60
 if not os.getenv('TURSO_DATABASE_URL') or not os.getenv('TURSO_AUTH_TOKEN'):raise RuntimeError('Remote credentials required')
 db=DB();db.init();install_wide(db)
 db.run("UPDATE misc_queue SET status='PARTIAL',next_retry=NULL WHERE status='RUNNING' AND julianday(last_attempt)<julianday('now','-2 hours')")
 if a.init_only:return
 import yfinance as yf
 out=Path('reports/fleet');out.mkdir(parents=True,exist_ok=True);failures=0
 members=refresh(db,out)
 if members is not None:seed(db,members)
 checkpoint(db,out,failures)
 for cid,s in db.run("SELECT c.id,c.symbol FROM misc_queue q JOIN dm_company c ON c.id=q.company_id WHERE c.active=1 AND q.dataset_group=? AND (q.next_retry IS NULL OR q.next_retry<=?) AND (q.dataset_group='EXTENDED' OR q.last_success IS NULL OR q.last_success<?) ORDER BY COALESCE(q.last_attempt,''),c.symbol LIMIT ?",(a.group,now(),(datetime.now(timezone.utc)-timedelta(days=7)).isoformat(),a.limit)):
  if time.monotonic()>=deadline or db.occupied()>=380000000 or db.storage()>=400000000:break
  cid=int(cid);db.run("UPDATE misc_queue SET status='RUNNING',attempts=attempts+1,last_attempt=? WHERE company_id=? AND dataset_group=?",(now(),cid,a.group))
  cov=[]
  try:
   modules=extended_plan(db,cid) if a.group=='EXTENDED' else None
   rows,cov,payload,errors=acquire(yf.Ticker,s,a.group,modules) if modules != [] else ([],[],{},[])
   if not rows and a.group=='CORE':raise RuntimeError('No usable data: '+ '; '.join(errors))
   if rows or payload or cov:publish(db,cid,s,rows,cov,payload,errors)
   if a.group=='EXTENDED':
    record_modules(db,cid,cov)
    if cov and all(x['status']=='RETRY_PENDING' for x in cov):raise RuntimeError('All attempted modules require retry')
   complete=a.group=='EXTENDED' and extended_status(db,cid)=='PUBLISHED_UNVERIFIED' or a.group=='CORE' and {r['statement'] for r in rows if r['frequency']=='ANNUAL'}=={'PNL','BS','CF'}
   status='PUBLISHED_UNVERIFIED' if complete else 'PARTIAL';success=now() if complete else None
  except Exception as e:status='FAILED';success=None;errors=[diagnostic(s,e)];failures+=1
  with (out/'attempts.jsonl').open('a') as f:f.write(json.dumps({'symbol':s,'group':a.group,'status':status,'errors':errors,'coverage':cov})+'\n')
  db.run('UPDATE misc_queue SET status=?,last_success=COALESCE(?,last_success),next_retry=?,error=? WHERE company_id=? AND dataset_group=?',(status,success,(datetime.now(timezone.utc)+timedelta(hours=6)).isoformat(),'; '.join(errors)[:500],cid,a.group))
  if a.group=='EXTENDED' and status!='FAILED':db.run('UPDATE misc_queue SET next_retry=(SELECT MIN(next_retry) FROM misc_module WHERE company_id=?) WHERE company_id=? AND dataset_group=?',(cid,cid,a.group))
  checkpoint(db,out,failures)
  print(s,status,flush=True)
  if failures>=5:break
 install_wide(db);print(checkpoint(db,out,failures))
 if failures>=5:raise RuntimeError('Repeated failures; retry deferred')
if __name__=='__main__':main()
