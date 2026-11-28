"""Incremental observed membership; one COMPLETE marker publishes all staged events."""
import csv,io,json,re,hashlib,os,time
from datetime import datetime,timezone
from pathlib import Path
import requests
from database import DB
ROOT=Path(__file__).parent

def now(): return datetime.now(timezone.utc).isoformat()
def init(db):
 db.init()
 for sql in (ROOT/'membership.sql').read_text().split(';'):
  if sql.strip(): db.run(sql)
 for r in json.loads((ROOT/'india_index_sources.json').read_text()):
  db.run('INSERT INTO dm_index(code,name,category) VALUES(?,?,?) ON CONFLICT(code) DO UPDATE SET name=excluded.name,category=excluded.category',(r['code'],r['name'],r['category']))
  iid=db.run('SELECT id FROM dm_index WHERE code=?',(r['code'],))[0][0]
  db.run('INSERT INTO dm_data_source(index_id,url,expected) VALUES(?,?,?) ON CONFLICT(index_id,url) DO UPDATE SET expected=excluded.expected',(iid,r['url'],r['expected']))
  for f in r['factors']:
   db.run('INSERT OR IGNORE INTO dm_factor(name) VALUES(?)',(f,));db.run('INSERT OR IGNORE INTO br_index_factor SELECT ?,id FROM dm_factor WHERE name=?',(iid,f))

def parse(text,expected=None,previous=None):
 reader=csv.DictReader(io.StringIO(text.lstrip('\ufeff'))); required={'Symbol','ISIN Code','Company Name','Industry'}
 reader.fieldnames=[h.strip().lstrip('\ufeff') for h in (reader.fieldnames or [])]
 if not required.issubset(reader.fieldnames or []): raise ValueError('Invalid CSV headers')
 result={}; symbols=set()
 for r in reader:
  isin,s,name,industry=(r[k].strip() for k in ['ISIN Code','Symbol','Company Name','Industry'])
  if not re.fullmatch(r'IN[A-Z0-9]{10}',isin) or not re.fullmatch(r'[A-Z0-9&._-]+',s) or not name or not industry or isin in result or s in symbols: raise ValueError('Invalid or duplicate constituent')
  result[isin]=(s,name,industry);symbols.add(s)
 n=len(result)
 if n<5 or (expected and not expected<=n<=expected+max(2,expected//50)) or (previous and n!=previous): raise ValueError(f'Unexpected count {n}; expected {expected}, previous {previous}; review required')
 return result

def guard(db):
 if db.occupied()>=380_000_000 or db.storage()>=400_000_000: raise RuntimeError('Storage safety threshold')

def publish(db,iid,sid,text,expected=None):
 guard(db); stamp=now()
 prev=db.run("SELECT id,sha,member_count FROM ft_index_snapshot WHERE index_id=? AND status='COMPLETE' ORDER BY id DESC LIMIT 1",(iid,))
 members=parse(text,expected,prev[0][2] if prev else None)
 # Unknown variable-size first snapshots cannot be independently certified; strict count on subsequent refreshes.
 sha=hashlib.sha256(json.dumps(members,sort_keys=True).encode()).hexdigest()
 if prev and prev[0][1]==sha: return 'UNCHANGED'
 byisin={r[0]:(r[1],r[2]) for r in db.run('SELECT isin,id,symbol FROM dm_company')}; bysymbol={v[1]:k for k,v in byisin.items()}; ids={}; conflicts=[]
 for isin,(s,name,industry) in members.items():
  if s in bysymbol and bysymbol[s]!=isin:
   db.run('INSERT OR REPLACE INTO misc_constituent_reconciliation VALUES(?,?,?,?,?,?)',(sid,isin,s,name,'SYMBOL_ISIN_CONFLICT',stamp));conflicts.append(s);continue
  if isin not in byisin:
   db.run('INSERT INTO dm_company(isin,symbol,name,industry,active) VALUES(?,?,?,?,0)',(isin,s,name,industry));byisin[isin]=(db.run('SELECT id FROM dm_company WHERE isin=?',(isin,))[0][0],s)
  ids[isin]=byisin[isin][0]
 if conflicts: raise ValueError('Identity conflicts: '+','.join(conflicts))
 old={r[0] for r in db.run('SELECT company_id FROM vw_company_index_membership_current WHERE index_id=?',(iid,))}; new=set(ids.values())
 snap=db.run("INSERT INTO ft_index_snapshot(index_id,source_id,observed,sha,member_count,status) VALUES(?,?,?,?,?,'STAGING') RETURNING id",(iid,sid,stamp,sha,len(new)))[0][0]
 events=[(snap,c,1) for c in sorted(new-old)]+[(snap,c,0) for c in sorted(old-new)]
 def batch(table,rows):
  for i in range(0,len(rows),100):
   guard(db); part=rows[i:i+100];db.run('INSERT INTO '+table+' VALUES '+','.join('(?,?,?)' for _ in part),tuple(v for row in part for v in row))
 batch('ft_index_event',events)
 if db.run('SELECT category FROM dm_index WHERE id=?',(iid,))[0][0]=='BENCHMARK':
  existing={r[0]:r[1] for r in db.run('SELECT company_id,industry FROM vw_company_industry_current')}; changes=[]
  for industry in sorted({v[2] for v in members.values()}):db.run('INSERT OR IGNORE INTO dm_industry(name) VALUES(?)',(industry,))
  industry_ids={r[1]:r[0] for r in db.run('SELECT id,name FROM dm_industry')}
  for isin,(_,_,industry) in members.items():
   cid=ids[isin]
   if existing.get(cid)!=industry:
    changes.append((snap,cid,industry_ids[industry]))
  batch('ft_classification_event',changes)
 guard(db)
 if db.run('SELECT count(*) FROM ft_index_event WHERE snapshot_id=?',(snap,))[0][0]!=len(events):raise RuntimeError('Event count mismatch')
 db.run("UPDATE ft_index_snapshot SET status='COMPLETE' WHERE id=?",(snap,));return 'PUBLISHED_UNVERIFIED'

def evidence(db):
 out=Path('reports/membership');out.mkdir(parents=True,exist_ok=True)
 for view in ['vw_index_source_health','vw_index_acquisition_progress','vw_index_constituents','vw_index_membership_changes','vw_company_membership_summary','misc_constituent_reconciliation','dm_factor','dm_industry','ft_index_snapshot']:
  (out/(view+'.json')).write_text(json.dumps(db.run('SELECT * FROM '+view),indent=2))
 (out/'storage.json').write_text(json.dumps(dict(allocated=db.storage(),occupied=db.occupied(),reusable=db.storage()-db.occupied())))

def main():
 if not os.getenv('TURSO_DATABASE_URL') or not os.getenv('TURSO_AUTH_TOKEN'):raise RuntimeError('Missing Turso configuration')
 db=DB();init(db);deadline=time.monotonic()+35*60
 # Abandoned staging events remain invisible; mark failed for audit.
 db.run("UPDATE ft_index_snapshot SET status='ABANDONED' WHERE status='STAGING'")
 try:
  for sid,iid,url,expected in db.run("SELECT id,index_id,url,expected FROM dm_data_source WHERE last_attempt IS NULL OR julianday(last_attempt)<julianday('now','-6 hours') AND (last_success IS NULL OR julianday(last_success)<julianday('now','-1 day') OR status='FAILED') ORDER BY COALESCE(last_attempt,''),id"):
   if time.monotonic()>deadline:break
   guard(db);db.run('UPDATE dm_data_source SET last_attempt=? WHERE id=?',(now(),sid))
   try:
    r=requests.get(url,headers={'User-Agent':'Mozilla/5.0','Accept':'text/csv,*/*'},timeout=45);r.raise_for_status()
    print(json.dumps({'source_id':sid,'http_status':r.status_code,'content_type':r.headers.get('Content-Type'),'bytes':len(r.content),'final_url':r.url}),flush=True)
    status=publish(db,iid,sid,r.content.decode('utf-8-sig'),expected)
    db.run('UPDATE dm_data_source SET status=?,last_success=?,error=NULL WHERE id=?',(status,now(),sid))
   except Exception as e:
    db.run("UPDATE dm_data_source SET status='FAILED',error=? WHERE id=?",(str(e)[:1000],sid))
   print(iid,db.run('SELECT status,error FROM dm_data_source WHERE id=?',(sid,)),flush=True)
 finally:evidence(db)
if __name__=='__main__':main()
