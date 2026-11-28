"""Bounded independent market acquisition. No financial or membership mutations."""
import argparse, datetime as dt, gzip, json, math, os, time, re
from pathlib import Path
from database import DB
from tickers import ticker_for
from fleet import normalize_extended

ROOT=Path(__file__).parent
# Explicit Yahoo aliases only; unsupported indices remain visible in coverage.
INDEX_TICKERS={'NIFTY_500':'^CRSLDX','NIFTY_50':'^NSEI','NIFTY_BANK':'^NSEBANK','NIFTY_IT':'^CNXIT','NIFTY_AUTO':'^CNXAUTO','NIFTY_FMCG':'^CNXFMCG','NIFTY_ENERGY':'^CNXENERGY','NIFTY_METAL':'^CNXMETAL','NIFTY_MEDIA':'^CNXMEDIA','NIFTY_PHARMA':'^CNXPHARMA','NIFTY_REALTY':'^CNXREALTY','NIFTY_PSU_BANK':'^CNXPSUBANK'}

def install(db):
 db.init()
 for sql in (ROOT/'membership.sql').read_text().split(';'):
  if sql.strip(): db.run(sql)
 for sql in (ROOT/'market.sql').read_text().split(';'):
  if sql.strip(): db.run(sql)

def number(x):
 if x is None:return None
 x=float(x)
 return x if math.isfinite(x) else None

def parse(payload):
 cols=payload['columns']; result={}
 if 'Close' not in cols:raise ValueError('Missing Close')
 if len(payload['index'])!=len(payload['data']):raise ValueError('Invalid payload length')
 for date,values in zip(payload['index'],payload['data']):
  text=str(date); parsed=dt.datetime.fromisoformat(text.replace('Z','+00:00')); day=(parsed.astimezone(dt.timezone(dt.timedelta(hours=5,minutes=30))) if parsed.tzinfo else parsed).date().isoformat(); d=dict(zip(cols,values))
  o,h,l,c,a,v=[number(d.get(k)) for k in ('Open','High','Low','Close','Adj Close','Volume')]
  if c is None:continue
  if any(x is not None and x<=0 for x in (o,h,l,c,a)) or (v is not None and v<0) or (h is not None and l is not None and (h<l or any(x is not None and not l<=x<=h for x in (o,c)))):raise ValueError('Invalid OHLCV '+day)
  row=(day,o,h,l,c,a,v)
  if day in result and result[day]!=row:raise ValueError('Conflicting duplicate date')
  result[day]=row
 return sorted(result.values())

def publish(db,kind,entity,rows,source):
 table='ft_company_daily_price' if kind=='COMPANY' else 'ft_index_daily_price'
 stamp=dt.datetime.now(dt.timezone.utc).isoformat(); changed=0
 for start in range(0,len(rows),60):
  batch=rows[start:start+60]; args=tuple(x for r in batch for x in (entity,*r,source,stamp))
  changed+=len(db.run(f'INSERT INTO {table} VALUES '+','.join(['(?,?,?,?,?,?,?,?,?,?)']*len(batch))+' ON CONFLICT(entity_id,trading_date,source) DO UPDATE SET open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,adjusted_close=excluded.adjusted_close,volume=excluded.volume,retrieved=excluded.retrieved WHERE open IS NOT excluded.open OR high IS NOT excluded.high OR low IS NOT excluded.low OR close IS NOT excluded.close OR adjusted_close IS NOT excluded.adjusted_close OR volume IS NOT excluded.volume RETURNING trading_date',args))
 return changed

def diagnostic(exc):
 text=str(exc)
 for key in ('TURSO_AUTH_TOKEN','TURSO_DATABASE_URL'):
  value=os.getenv(key)
  if value:text=text.replace(value,'[REDACTED]')
 return re.sub(r'https?://[^\s]+','[URL]',text)[:800]

def download(yf,ticker,start,end,deadline):
 for attempt in range(3):
  try:
   return yf.Ticker(ticker).history(start=start,end=end,auto_adjust=False,actions=False,timeout=20)
  except Exception as exc:
   msg=(type(exc).__name__+' '+str(exc)).lower()
   if not any(x in msg for x in ('timeout','connection','429','rate limit','ratelimit','502','503','504')) or attempt==2 or time.monotonic()+2**attempt>=deadline:raise
   time.sleep(2**attempt)

def run(db,limit=20,minutes=35,recovery_limit=5,yf=None):
 install(db); deadline=time.monotonic()+minutes*60; today=dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes=30))).date(); cutoff=(today-dt.timedelta(days=400)).isoformat(); stamp=dt.datetime.now(dt.timezone.utc).isoformat()
 # Include all current constituents, not just the active benchmark universe.
 companies=db.run('SELECT c.id,c.symbol,t.ticker FROM dm_company c LEFT JOIN dm_ticker t ON t.company_id=c.id WHERE c.active=1 OR c.id IN (SELECT company_id FROM vw_index_constituents)')
 indices=db.run('SELECT id,code FROM dm_index'); entities=[('COMPANY',i,t or ticker_for(s)) for i,s,t in companies]+[('INDEX',i,INDEX_TICKERS.get(s)) for i,s in indices]
 states={(k,i):(retry,pid,status) for k,i,retry,pid,status in db.run('SELECT kind,entity_id,next_retry,payload_id,status FROM misc_market_data_queue')}
 def due(entity):
  retry=states.get(entity[:2],(None,None,None))[0]
  return not retry or dt.datetime.fromisoformat(retry.replace('Z','+00:00'))<=dt.datetime.fromisoformat(stamp)
 eligible=[e for e in entities if due(e)]
 eligible.sort(key=lambda x:(states.get(x[:2],('',None,None))[0] or '',x[0],x[1]))
 failed=[e for e in eligible if states.get(e[:2],(None,None,None))[2] not in (None,'AVAILABLE','NO_CONFIGURED_INDEX_SOURCE') and e[2]]
 # Reserve a bounded recovery batch, then continue ordinary coverage.
 recovery=failed[:min(limit,recovery_limit)]
 entities=recovery+[e for e in eligible if e not in recovery and e not in failed]+failed[len(recovery):]
 report={'eligible':len(companies)+len(indices),'due':len(eligible),'attempted':0,'changed':0,'errors':[],'warnings':[],'attempts':[]}
 if yf is None:
  import yfinance as yf
 if hasattr(yf,'config') and hasattr(yf.config,'debug'):yf.config.debug.hide_exceptions=False
 symbols={('COMPANY',i):s for i,s,t in companies}|{('INDEX',i):s for i,s in indices}
 for kind,i,ticker in entities:
  if report['attempted']>=limit or time.monotonic()>=deadline:break
  retry,pid,previous=states.get((kind,i),(None,None,None))
  if db.occupied()>=380000000 or db.storage()>=400000000:report['guard']='STORAGE_GUARD_TRIGGERED';break
  report['attempted']+=1; status='AVAILABLE'; delay=1; stage='SOURCE_MAPPING'; before=report['changed']; info={'kind':kind,'id':i,'symbol':symbols[(kind,i)],'ticker':ticker,'previous_status':previous}
  try:
   if not ticker:raise ValueError('NO_CONFIGURED_INDEX_SOURCE')
   stage='PAYLOAD_LOOKUP'
   if kind=='COMPANY' and pid is None:
    old=db.run("SELECT e.payload_id,p.payload FROM dm_snapshot s JOIN ft_extended e ON e.snapshot_id=s.id JOIN dm_payload p ON p.id=e.payload_id WHERE s.company_id=? AND s.ticker=? AND s.status='COMPLETE' AND e.module='prices' ORDER BY s.id DESC LIMIT 1",(i,ticker))
    if old:
     try:rows=parse(json.loads(gzip.decompress(old[0][1])))
     except Exception as exc:report['warnings'].append({**info,'stage':'PAYLOAD_PARSE','error':diagnostic(exc)})
     else:
      stage='PAYLOAD_PUBLICATION'; report['changed']+=publish(db,kind,i,[r for r in rows if r[0]>=cutoff],'YAHOO:'+ticker);pid=old[0][0]
   stage='DATABASE_COVERAGE'
   table='ft_company_daily_price' if kind=='COMPANY' else 'ft_index_daily_price'
   first,last=db.run(f'SELECT min(trading_date),max(trading_date) FROM {table} WHERE entity_id=? AND source=?',(i,'YAHOO:'+ticker))[0]
   start=cutoff if not first or first>(today-dt.timedelta(days=365)).isoformat() else max(cutoff,(dt.date.fromisoformat(last)-dt.timedelta(days=7)).isoformat())
   stage='DOWNLOAD'
   frame=download(yf,ticker,start,(today+dt.timedelta(days=int(dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes=30))).hour>=18))).isoformat(),deadline)
   stage='VALIDATION'; rows=parse(normalize_extended(frame) or {'columns':['Close'],'index':[],'data':[]})
   if not rows:raise ValueError('EMPTY_OR_UNAVAILABLE')
   if kind=='INDEX':rows=[(*r[:-1],None) for r in rows]
   stage='DATABASE_PUBLICATION';report['changed']+=publish(db,kind,i,rows,'YAHOO:'+ticker)
  except Exception as exc:
   status={'SOURCE_MAPPING':'NO_CONFIGURED_INDEX_SOURCE','DOWNLOAD':'SOURCE_UNAVAILABLE','VALIDATION':'VALIDATION_FAILED'}.get(stage,'DATABASE_FAILED');delay=7 if not ticker else 1
   report['errors'].append({**info,'stage':stage,'error_type':type(exc).__name__,'error':diagnostic(exc),'status':status})
  next_retry=(dt.datetime.now(dt.timezone.utc)+dt.timedelta(days=delay)).isoformat()
  try:
   db.run('INSERT INTO misc_market_data_queue VALUES(?,?,?,?,?,?) ON CONFLICT(kind,entity_id) DO UPDATE SET status=excluded.status,next_retry=excluded.next_retry,payload_id=excluded.payload_id,last_attempt=excluded.last_attempt',(kind,i,status,next_retry,pid,stamp))
  except Exception as exc:
   report['errors'].append({**info,'stage':'QUEUE_PUBLICATION','error_type':type(exc).__name__,'error':diagnostic(exc),'status':'DATABASE_FAILED'});status='DATABASE_FAILED'
  report['attempts'].append({**info,'status':status,'changed':report['changed']-before})
  if status=='DATABASE_FAILED':report['guard']='DATABASE_FAILURE_STOP';break
 report['outcome']=report.get('guard') or ('PARTIAL_PROGRESS' if report['errors'] and report['changed'] else 'SOURCE_UNAVAILABLE' if report['errors'] else 'SUCCESS_NEW_DATA' if report['changed'] else 'SUCCESS_UNCHANGED' if report['attempted'] else 'NO_ELIGIBLE_WORK')
 return report

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--limit',type=int,default=60);p.add_argument('--minutes',type=int,default=35);p.add_argument('--recovery-limit',type=int,default=5);a=p.parse_args()
 if not os.getenv('TURSO_DATABASE_URL') or not os.getenv('TURSO_AUTH_TOKEN'):raise SystemExit('Missing database configuration')
 out=Path('reports/market');out.mkdir(parents=True,exist_ok=True);r=run(DB(),a.limit,a.minutes,a.recovery_limit);(out/'summary.json').write_text(json.dumps(r,indent=2));print(r)
