import os, sqlite3, json, csv, io, hashlib, datetime as dt, time, argparse
from urllib.parse import urlparse, urljoin
import requests
from defusedxml import ElementTree as ET
from decimal import Decimal, InvalidOperation

UTC=dt.timezone.utc
UNIVERSE='https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv'
BASE='https://www.nseindia.com'
SCHEMA=[
'''CREATE TABLE IF NOT EXISTS companies(isin TEXT PRIMARY KEY,symbol TEXT NOT NULL,name TEXT,industry TEXT,active INTEGER DEFAULT 1)''',
'''CREATE TABLE IF NOT EXISTS membership(isin TEXT,day TEXT,PRIMARY KEY(isin,day))''',
'''CREATE TABLE IF NOT EXISTS tasks(isin TEXT,dataset TEXT,year INTEGER,status TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,next_try TEXT DEFAULT '',error TEXT,PRIMARY KEY(isin,dataset,year))''',
'''CREATE TABLE IF NOT EXISTS filings(id TEXT PRIMARY KEY,isin TEXT,exchange TEXT,dataset TEXT,published TEXT,metadata TEXT,discovered TEXT)''',
'''CREATE TABLE IF NOT EXISTS documents(filing TEXT,url TEXT,sha TEXT,bytes INTEGER,status TEXT,PRIMARY KEY(filing,url,sha))''',
'''CREATE TABLE IF NOT EXISTS facts(filing TEXT,sha TEXT,concept TEXT,context TEXT,unit TEXT,value TEXT,decimals TEXT,PRIMARY KEY(filing,sha,concept,context,unit,value))''',
'''CREATE INDEX IF NOT EXISTS facts_filing ON facts(filing)''',
'''CREATE INDEX IF NOT EXISTS tasks_ready ON tasks(status,next_try,year)''',
'''CREATE VIEW IF NOT EXISTS financial_facts AS SELECT f.*,r.isin,r.exchange,r.dataset,r.published FROM facts f JOIN filings r ON r.id=f.filing''']

def now(): return dt.datetime.now(UTC).isoformat()
def digest(x): return hashlib.sha256(x).hexdigest()
def dump(x): return json.dumps(x,sort_keys=True,ensure_ascii=False,separators=(',',':'))
class DB:
    def __init__(self,local=None):
        self.local=sqlite3.connect(local) if local else None
        if not local:
            self.url=os.environ['TURSO_DATABASE_URL'].replace('libsql://','https://',1).rstrip('/')+'/v2/pipeline'
            if urlparse(self.url).scheme!='https': raise ValueError('HTTPS required')
            self.session=requests.Session(); self.session.headers['Authorization']='Bearer '+os.environ['TURSO_AUTH_TOKEN']
    def batch(self,items):
        if self.local:
            with self.local:
                return [self.local.execute(s,a).fetchall() for s,a in items]
        def arg(v):
            return {'type':'null'} if v is None else {'type':'integer','value':str(v)} if isinstance(v,int) else {'type':'text','value':str(v)}
        payload={'requests':[{'type':'execute','stmt':{'sql':s,'args':[arg(v) for v in a]}} for s,a in items]+[{'type':'close'}]}
        r=self.session.post(self.url,json=payload,timeout=60); r.raise_for_status(); results=r.json()['results']; out=[]
        for result in results[:-1]:
            if result['type']!='ok': raise RuntimeError(result.get('error',{}).get('message','Database error'))
            out.append([tuple(c.get('value') for c in row) for row in result['response']['result']['rows']])
        return out
    def run(self,s,*a): return self.batch([(s,a)])[0]
    def init(self): self.batch([(s,()) for s in SCHEMA])
class SourceError(RuntimeError):
    def __init__(self,message,systemic=False):
        super().__init__(message); self.systemic=systemic

class Source:
    def __init__(self): self.s=requests.Session(); self.s.headers.update({'User-Agent':'Fundamental_ind research pipeline/1.0','Accept':'*/*'})
    def get(self,url,params=None):
        for hop in range(4):
            host=urlparse(url).hostname or ''
            if urlparse(url).scheme!='https' or not any(host==d or host.endswith('.'+d) for d in ('nseindia.com','nseindia.in','niftyindices.com','bseindia.com')): raise SourceError('Unapproved source URL')
            time.sleep(0.8)
            try:
                r=self.s.get(url,params=params,timeout=30,allow_redirects=False)
            except requests.RequestException as e:
                raise SourceError(f'{host}: {type(e).__name__}',True) from e
            if r.status_code in (301,302,303,307,308):
                target=r.headers.get('Location','')
                if not target: raise SourceError(f'{host}: redirect without location',True)
                url=urljoin(r.url,target); params=None
                continue
            if r.status_code!=200:
                raise SourceError(f'{host}{urlparse(url).path}: HTTP {r.status_code}',r.status_code in (401,403,429,500,502,503,504))
            if len(r.content)>30_000_000: raise SourceError('Document exceeds limit')
            return r.content
        raise SourceError('Too many source redirects',True)
    def rows(self,path,params):
        try: data=json.loads(self.get(BASE+path,params))
        except (ValueError,UnicodeError) as e: raise SourceError(f'{path}: non-JSON response',True) from e
        if not isinstance(data,list) or any(not isinstance(x,dict) for x in data): raise SourceError(f'{path}: unexpected exchange response schema',True)
        return data

def parse_xml(raw):
    root=ET.fromstring(raw); ns='{http://www.xbrl.org/2003/instance}'
    if root.tag!=ns+'xbrl': raise ValueError('Not an XBRL instance')
    contexts={e.attrib['id']:ET.tostring(e,encoding='unicode') for e in root.findall(ns+'context')}
    units={e.attrib['id']:ET.tostring(e,encoding='unicode') for e in root.findall(ns+'unit')}
    facts=[]
    for e in root.iter():
        c=e.get('contextRef'); u=e.get('unitRef'); value=(e.text or '').strip()
        if c and c in contexts and value:
            facts.append((e.tag,contexts[c],units.get(u,u or ''),value,e.get('decimals')))
    if not facts: raise ValueError('No contextual facts')
    return facts

def refresh(db,source):
    rows=list(csv.DictReader(io.StringIO(source.get(UNIVERSE).decode('utf-8-sig')))); rows=[{k.strip():v.strip() for k,v in r.items() if k and v is not None} for r in rows]
    if not 450<=len(rows)<=550 or len({r['ISIN Code'] for r in rows})!=len(rows): raise ValueError('Invalid universe size or duplicate ISIN')
    year=dt.date.today().year
    for r in rows:
        isin=r['ISIN Code']; db.batch([('INSERT INTO companies(isin,symbol,name,industry) VALUES(?,?,?,?) ON CONFLICT(isin) DO UPDATE SET symbol=excluded.symbol,name=excluded.name,industry=excluded.industry,active=1',(isin,r['Symbol'],r['Company Name'],r['Industry'])),('INSERT OR IGNORE INTO membership VALUES(?,?)',(isin,dt.date.today().isoformat()))]+[('INSERT OR IGNORE INTO tasks(isin,dataset,year) VALUES(?,?,?)',(isin,d,y)) for d in ('quarterly','annual','shareholding','actions','announcements') for y in range(year-15,year+1)])
    db.run('UPDATE companies SET active=0 WHERE isin NOT IN (SELECT isin FROM membership WHERE day=?)',dt.date.today().isoformat())

def discover(source,symbol,dataset,year):
    p={'index':'equities','symbol':symbol,'from_date':f'01-01-{year}','to_date':f'31-12-{year}'}
    if year==dt.date.today().year: p['to_date']=dt.date.today().strftime('%d-%m-%Y')
    paths={'quarterly':'corporates-financial-results','annual':'corporates-financial-results','shareholding':'corporate-share-holdings-master','actions':'corporates-corporateActions','announcements':'corporate-announcements'}
    if dataset in ('quarterly','annual'): p['period']=dataset.title()
    return source.rows('/api/'+paths[dataset],p)

def ingest(db,source,isin,symbol,dataset,year):
    rows=discover(source,symbol,dataset,year); failures=0
    for row in rows:
        if row.get('symbol') and row['symbol'].upper()!=symbol.upper(): continue
        meta=dump(row); fid=digest((isin+dataset+meta).encode()); published=row.get('broadCastDate') or row.get('broadcastDate') or row.get('filingDate')
        db.run('INSERT OR IGNORE INTO filings VALUES(?,?,?,?,?,?,?)',fid,isin,'NSE',dataset,published,meta,now())
        for key,val in row.items():
            if not isinstance(val,str) or not val.startswith('https://') or not any(x in key.lower() for x in ('xbrl','attach','pdf')): continue
            if db.run("SELECT 1 FROM documents WHERE filing=? AND url=? AND status='parsed' LIMIT 1",fid,val): continue
            try:
                raw=source.get(val); sha=digest(raw)
                if raw.startswith(b'%PDF'): status='metadata_only_pdf'
                else:
                    facts=parse_xml(raw)
                    for i in range(0,len(facts),100): db.batch([('INSERT OR IGNORE INTO facts VALUES(?,?,?,?,?,?,?)',(fid,sha,*f)) for f in facts[i:i+100]])
                    status='parsed'
                db.run('INSERT OR IGNORE INTO documents VALUES(?,?,?,?,?)',fid,val,sha,len(raw),status)
            except Exception as e:
                failures+=1; print('Document failure',type(e).__name__,str(e)[:300]);
                if isinstance(e,SourceError) and e.systemic: raise
    if failures: raise RuntimeError(f'{failures} document failures')
    return len(rows)

def work(db,source,limit):
    tasks=db.run("SELECT t.isin,c.symbol,t.dataset,t.year,t.attempts FROM tasks t JOIN companies c ON c.isin=t.isin WHERE c.active=1 AND t.next_try<=? AND t.status IN ('pending','retry','refresh') ORDER BY CASE WHEN t.status='refresh' THEN 1 ELSE 0 END,t.attempts,t.year DESC,t.isin LIMIT ?",now(),limit)
    errors=0
    for isin,symbol,dataset,year,attempts in tasks:
        year=int(year); attempts=int(attempts)
        last_error=None
        try:
            n=ingest(db,source,isin,symbol,dataset,year); status='refresh' if year>=dt.date.today().year-1 else 'done'; delay=1 if status=='refresh' else 365; err=None; attempts=0
            print(symbol,dataset,year,n,'discovered')
        except Exception as e:
            last_error=e
            errors+=1; status='retry'; attempts+=1; delay=min(30,2**min(attempts,5)); err=f'{type(e).__name__}: {str(e)[:400]}' if isinstance(e,SourceError) else type(e).__name__; print(symbol,dataset,year,err)
        db.run('UPDATE tasks SET status=?,attempts=?,next_try=?,error=? WHERE isin=? AND dataset=? AND year=?',status,attempts,(dt.datetime.now(UTC)+dt.timedelta(days=delay)).isoformat(),err,isin,dataset,year)
        if status=='retry' and isinstance(last_error,SourceError) and last_error.systemic:
            print('Source circuit breaker: remaining tasks left untouched'); break
    if errors: raise RuntimeError(f'{errors} tasks failed; retry persisted')

def main():
    p=argparse.ArgumentParser(); p.add_argument('command',choices=['init','refresh','work','report']); p.add_argument('--local'); p.add_argument('--limit',type=int,default=30); a=p.parse_args(); db=DB(a.local); db.init()
    if a.command=='refresh': refresh(db,Source())
    elif a.command=='work': work(db,Source(),a.limit)
    elif a.command=='report':
        report={t:db.run('SELECT COUNT(*) FROM '+t)[0][0] for t in ('companies','filings','documents','facts')}; report['tasks']=db.run('SELECT status,COUNT(*) FROM tasks GROUP BY status'); print(dump(report))
if __name__=='__main__': main()
