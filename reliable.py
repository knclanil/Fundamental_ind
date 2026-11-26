"""Additive ingestion layer. Legacy tables are retained, never certified."""
import datetime as dt, json, csv, os, re, argparse, io
from decimal import Decimal, InvalidOperation
from quality import audit, storage
from pipeline import DB, Source, SourceError, ET, digest, dump, now, UNIVERSE, UTC

DDL=[
'''CREATE TABLE IF NOT EXISTS v2_jobs(isin TEXT,source TEXT,dataset TEXT,year INTEGER,state TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,due TEXT DEFAULT '',detail TEXT,PRIMARY KEY(isin,source,dataset,year))''',
'''CREATE TABLE IF NOT EXISTS v2_filings(id TEXT PRIMARY KEY,isin TEXT,source TEXT,dataset TEXT,url TEXT,sha TEXT,published TEXT,retrieved TEXT,metadata TEXT,status TEXT)''',
'''CREATE TABLE IF NOT EXISTS v2_facts(id TEXT PRIMARY KEY,filing TEXT,concept TEXT,entity TEXT,start TEXT,end TEXT,dimensions TEXT,unit TEXT,value TEXT,decimals TEXT,scope TEXT,status TEXT)''',
'''CREATE INDEX IF NOT EXISTS v2_fact_filing ON v2_facts(filing)''',
'''CREATE TABLE IF NOT EXISTS v2_listings(isin TEXT,source TEXT,code TEXT,PRIMARY KEY(isin,source))''',
'''CREATE TABLE IF NOT EXISTS v2_issues(filing TEXT,rule TEXT,detail TEXT,PRIMARY KEY(filing,rule,detail))''',
'''CREATE INDEX IF NOT EXISTS v2_job_due ON v2_jobs(state,due,year)''',
'''CREATE VIEW IF NOT EXISTS v2_financial_data AS SELECT f.*,d.isin,d.source,d.dataset,d.url,d.sha,d.published,d.retrieved FROM v2_facts f JOIN v2_filings d ON d.id=f.filing WHERE d.status='parsed' AND f.status='validated' AND d.dataset IN ('legacy','integrated','results')''']
NS='{http://www.xbrl.org/2003/instance}'
XSI='{http://www.w3.org/2001/XMLSchema-instance}'
NSE={'legacy':'corporates-financial-results','integrated':'integrated-filing-results','shareholding':'corporate-share-holdings-master','actions':'corporates-corporateActions','announcements':'corporate-announcements','annual_reports':'annual-reports'}
BSE={'results':'FinancialResult/w','announcements':'AnnGetData/w'}

def init(db):
    db.init()
    for s in DDL: db.run(s)
    db.run("CREATE TABLE IF NOT EXISTS v2_migrations(name TEXT PRIMARY KEY)")
    db.run("UPDATE v2_jobs SET state='not_applicable' WHERE source='NSE' AND dataset='integrated' AND year<2025 AND state='pending'")
    if not db.run("SELECT 1 FROM v2_migrations WHERE name='nse_routes_fix'"):
        db.run("UPDATE v2_jobs SET due='',state='pending',attempts=0 WHERE source='NSE' AND dataset IN ('legacy','integrated','annual_reports') AND state IN ('retry','empty','metadata_only','partial')")
        db.run("INSERT OR IGNORE INTO v2_migrations VALUES('nse_routes_fix')")

def chunks(db,items,n=75):
    for i in range(0,len(items),n): db.batch(items[i:i+n])

def parse(raw,scope='unknown'):
    root=ET.fromstring(raw)
    if root.tag!=NS+'xbrl': raise ValueError('Unsupported document: expected XBRL instance')
    # Expand QName-valued units and dimensions using in-scope namespace bindings.
    bindings={}; stack=[]; pending=[]; maps={}
    for event,obj in ET.iterparse(io.BytesIO(raw),events=('start','end','start-ns')):
        if event=='start-ns': pending.append(obj)
        elif event=='start':
            stack.append(bindings); bindings=dict(bindings); bindings.update(pending); pending=[]; maps[id(obj)]=bindings
        else: bindings=stack.pop()
    # iterparse nodes are distinct from fromstring nodes; use the iterparse root instead.
    root=obj
    def q(text,node):
        text=(text or '').strip(); prefix,sep,name=text.partition(':'); m=maps[id(node)]
        return '{'+m.get(prefix,'?')+'}'+name if sep else '{'+m.get('','')+'}'+prefix
    contexts={}
    for c in root.findall(NS+'context'):
        entity=c.find('.//'+NS+'identifier'); p=c.find(NS+'period')
        if p is None: continue
        start=p.findtext(NS+'startDate'); end=p.findtext(NS+'endDate') or p.findtext(NS+'instant')
        dims=[]
        for e in c.iter():
            if e.tag.endswith('}explicitMember'): dims.append((q(e.get('dimension'),e),q(e.text,e)))
            elif e.tag.endswith('}typedMember'): dims.append((q(e.get('dimension'),e),ET.tostring(e,encoding='unicode')))
        contexts[c.get('id')]=(entity.text if entity is not None else None,start,end,dump(sorted(dims)))
    units={}
    for u in root.findall(NS+'unit'):
        div=u.find(NS+'divide')
        def measures(node): return sorted(q(e.text,e) for e in node.iter(NS+'measure'))
        units[u.get('id')]=dump([measures(u),[]] if div is None else [measures(div.find(NS+'unitNumerator')),measures(div.find(NS+'unitDenominator'))])
    out=[]
    for e in root.iter():
        if not e.get('contextRef'): continue
        entity,start,end,dims=contexts.get(e.get('contextRef'),(None,None,None,'[]')); unit=units.get(e.get('unitRef')); value=(e.text or '').strip(); status='validated'
        if e.get(XSI+'nil') in ('true','1'): value=None; status='unverified'
        else:
            try:
                if not Decimal(value).is_finite(): raise InvalidOperation()
            except InvalidOperation: status='unverified'
        try:
            if not end or (start and dt.date.fromisoformat(start)>dt.date.fromisoformat(end)): raise ValueError()
            dt.date.fromisoformat(end)
        except ValueError: status='quarantined'
        if status=='validated' and (not entity or not unit or scope=='unknown' or '?' in unit or '?' in dims): status='unverified'
        out.append((e.tag,entity,start,end,dims,unit,value,e.get('decimals'),scope,status))
    if not out: raise ValueError('No contextual facts')
    return out

def refresh(db,source):
    rows=list(csv.DictReader(io.StringIO(source.get(UNIVERSE).decode('utf-8-sig')))); rows=[{k.strip():v.strip() for k,v in r.items() if k and v is not None} for r in rows]
    if not 450<=len(rows)<=550 or len({r['ISIN Code'] for r in rows})!=len(rows): raise ValueError('Invalid universe')
    day=dt.date.today().isoformat(); year=dt.date.today().year
    for r in rows:
        isin=r['ISIN Code']; symbol=r['Symbol']
        db.batch([('INSERT INTO companies(isin,symbol,name,industry,active) VALUES(?,?,?,?,1) ON CONFLICT(isin) DO UPDATE SET symbol=excluded.symbol,name=excluded.name,industry=excluded.industry,active=1',(isin,symbol,r['Company Name'],r['Industry'])),('INSERT OR IGNORE INTO membership VALUES(?,?)',(isin,day)),('INSERT INTO v2_listings VALUES(?,?,?) ON CONFLICT(isin,source) DO UPDATE SET code=excluded.code',(isin,'NSE',symbol))])
    db.run('UPDATE companies SET active=0 WHERE isin NOT IN (SELECT isin FROM membership WHERE day=?)',day)
    # Official BSE listing candidate; require exact ISIN, never name matching.
    try:
        data=json.loads(source.get('https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w',{'Group':'','Scripcode':'','industry':'','segment':'Equity','status':'Active'}))
        if not isinstance(data,list): raise ValueError('BSE listing schema')
        known={r['ISIN Code'] for r in rows}; items=[]
        for r in data:
            isin=r.get('ISIN_NUMBER') or r.get('ISIN') or r.get('ISIN No') or r.get('ISIN_NO'); code=str(r.get('SCRIP_CD') or r.get('SCRIP_CODE') or r.get('ScripCode') or r.get('Scrip Code') or '')
            if isin in known and code.isdigit(): items.append(('INSERT INTO v2_listings VALUES(?,?,?) ON CONFLICT(isin,source) DO UPDATE SET code=excluded.code',(isin,'BSE',code)))
        chunks(db,items); print('BSE mappings',len(items))
    except Exception as e: print('BSE listing unavailable:',str(e)[:250])
    for exchange,ds in [('NSE',NSE),('BSE',BSE)]:
        for dataset in ds:
            for y in range(year-15,year+1):
                if exchange=='NSE' and dataset=='integrated' and y<2025: continue
                db.run('INSERT OR IGNORE INTO v2_jobs(isin,source,dataset,year) SELECT l.isin,l.source,?,? FROM v2_listings l JOIN companies c ON c.isin=l.isin WHERE l.source=? AND c.active=1',dataset,y,exchange)

def discover(source,exchange,code,dataset,year):
    end=min(dt.date(year,12,31),dt.date.today()); start=dt.date(year,1,1)
    if exchange=='NSE':
        base='https://www.nseindia.in/api/'+NSE[dataset]
        params={'index':'equities','symbol':code,'from_date':start.strftime('%d-%m-%Y'),'to_date':end.strftime('%d-%m-%Y')}
        if dataset=='integrated':
            if year<2025: return []
            rows=[]
            for page in range(1,101):
                data=json.loads(source.get(base,dict(params,type='Integrated Filing- Financials',page=page,size=20)))
                if not isinstance(data,dict) or not isinstance(data.get('data'),list) or 'totalCount' not in data: raise SourceError('Unexpected integrated pagination schema',True)
                batch=data['data']; total=int(data['totalCount'])
                if any(not isinstance(r,dict) for r in batch): raise SourceError('Invalid integrated record',True)
                if not batch and len(rows)<total: raise SourceError('Incomplete integrated pagination',True)
                rows.extend(batch)
                if len(rows)>=total: return rows
            raise SourceError('Integrated pagination limit exceeded',True)
        if dataset=='legacy':
            rows=[]
            for period in ('Quarterly','Annual','Half-Yearly'):
                batch=json.loads(source.get(base,dict(params,period=period)))
                if not isinstance(batch,list) or any(not isinstance(r,dict) for r in batch): raise SourceError('Unexpected legacy schema',True)
                rows.extend(batch)
            return rows
        if dataset=='annual_reports':
            data=json.loads(source.get(base,{'index':'equities','symbol':code}))
            if isinstance(data,dict): data=data.get('data')
            if not isinstance(data,list) or any(not isinstance(r,dict) for r in data): raise SourceError('Unexpected annual report schema',True)
            return [r for r in data if str(r.get('toYr'))==str(year)]
        data=json.loads(source.get(base,params))
    else:
        data=json.loads(source.get('https://api.bseindia.com/BseIndiaAPI/api/'+BSE[dataset],{'scripcode':code,'strScrip':code,'strPrevDate':start.strftime('%Y%m%d'),'strToDate':end.strftime('%Y%m%d'),'pageno':1,'strCat':'-1','strType':'C','subcategory':'-1'}))
    if isinstance(data,dict): data=data.get('data',data.get('Table'))
    if not isinstance(data,list) or any(not isinstance(r,dict) for r in data): raise SourceError('Unexpected discovery schema',True)
    return data

def ingest(db,source,isin,exchange,code,dataset,year):
    if exchange=='NSE' and dataset=='integrated' and year<2025: return 'not_applicable','Integrated filing begins in 2025'
    rows=discover(source,exchange,code,dataset,year); parsed=0; metadata=0; failed=0
    for row in rows:
        reported=(row.get('symbol') or row.get('Symbol')) if exchange=='NSE' else row.get('SCRIP_CD') or row.get('SCRIP_CODE') or row.get('ScripCode')
        # Fail closed if the response cannot be attributed to the requested company.
        if reported is None or str(reported).upper()!=code.upper():
            print('Unattributed discovery row',exchange,dataset,sorted(row.keys())); continue
        meta=dump(row); scope=str(row.get('consolidated') or row.get('consolidation') or row.get('resultType') or row.get('consolidatedFlag') or '').lower(); scope={'consolidated':'consolidated','standalone':'standalone','non-consolidated':'standalone','non consolidated':'standalone'}.get(scope,'unknown')
        pub=row.get('broadcast_Date') or row.get('broadcastDate') or row.get('broadCastDate') or row.get('filingDate')
        # Only ISO dates are safe for lexical as-of queries; original retained in metadata.
        try: pub=dt.datetime.fromisoformat(pub).isoformat() if pub and 'T' in pub else dt.date.fromisoformat(pub).isoformat() if pub else None
        except (ValueError,TypeError): pub=None
        urls={v for k,v in row.items() if isinstance(v,str) and v.startswith('https://') and any(x in k.lower() for x in ('xbrl','attach','pdf','xml','filename'))}
        mid=digest(dump([isin,exchange,dataset,meta]).encode()); db.run('INSERT OR IGNORE INTO v2_filings VALUES(?,?,?,?,?,?,?,?,?,?)',mid,isin,exchange,dataset,None,None,pub,now(),meta,'metadata'); metadata+=1
        for url in sorted(urls):
            try:
                raw=source.get(url); sha=digest(raw); fid=digest(dump([mid,url,sha]).encode())
                old=db.run('SELECT status FROM v2_filings WHERE id=?',fid)
                if old and old[0][0]=='parsed': parsed+=1; continue
                db.run('INSERT OR IGNORE INTO v2_filings VALUES(?,?,?,?,?,?,?,?,?,?)',fid,isin,exchange,dataset,url,sha,pub,now(),meta,'pending')
                if raw.startswith(b'%PDF'):
                    db.run("UPDATE v2_filings SET status='metadata_only_pdf' WHERE id=?",fid); continue
                facts=parse(raw,scope); items=[]
                for f in facts:
                    key=digest(dump([fid,*f]).encode()); items.append(('INSERT OR IGNORE INTO v2_facts VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(key,fid,*f)))
                chunks(db,items); db.run("UPDATE v2_filings SET status='parsed' WHERE id=?",fid); parsed+=1
            except Exception as e:
                failed+=1; db.run('INSERT OR IGNORE INTO v2_issues VALUES(?,?,?)',mid,'document_failure',str(e)[:400]); print('document failure',str(e)[:200])
                if isinstance(e,SourceError) and e.systemic: raise
    if failed: return 'partial',f'{parsed} parsed; {failed} failed; {metadata} metadata'
    if parsed: return 'parsed',f'{parsed} documents parsed; {metadata} metadata'
    return ('metadata_only' if metadata else 'empty'),f'{metadata} metadata; no parsed documents'

def work(db,source,limit):
    usage=storage(db)['fraction']
    if usage is not None and usage>=1: raise RuntimeError('Configured database storage budget reached')
    if usage is not None and usage>=.70: print('Storage budget warning',usage)
    year=dt.date.today().year; blocked=set(); errors=0; processed=0
    # Reserve half of each batch for historical work to prevent starvation.
    for recent,budget in [(True,(limit+1)//2),(False,limit//2)]:
        if not recent and usage is not None and usage>=.95: continue
        allocation=max(1,budget//4)
        for exchange,datasets in [('NSE',NSE),('BSE',BSE)]:
            for dataset in datasets:
                if budget<=0: break
                jobs=db.run('SELECT j.isin,l.code,j.year,j.attempts FROM v2_jobs j JOIN v2_listings l ON l.isin=j.isin AND l.source=j.source JOIN companies c ON c.isin=j.isin WHERE c.active=1 AND j.source=? AND j.dataset=? AND j.state<>? AND j.due<=? AND j.year '+('>=?' if recent else '<?')+' ORDER BY j.due,j.year DESC,j.isin LIMIT ?',exchange,dataset,'not_applicable',now(),year-1,min(budget,allocation if dataset in ('legacy','integrated','results') else max(1,allocation//4)))
                for isin,code,y,attempts in jobs:
                    if (exchange,dataset) in blocked or budget<=0: break
                    budget-=1; processed+=1; y=int(y); attempts=int(attempts)
                    try:
                        state,detail=ingest(db,source,isin,exchange,code,dataset,y); delay=1 if recent else 90; attempts=0
                        if state in ('empty','partial','metadata_only'): delay=7 if recent else 30
                    except Exception as e:
                        state='retry'; detail=str(e)[:400]; attempts+=1; delay=min(30,2**min(attempts,5)); errors+=1
                        if isinstance(e,SourceError) and (e.systemic or 'HTTP 404' in str(e)): blocked.add((exchange,dataset))
                    db.run('UPDATE v2_jobs SET state=?,detail=?,attempts=?,due=? WHERE isin=? AND source=? AND dataset=? AND year=?',state,detail,attempts,(dt.datetime.now(UTC)+dt.timedelta(days=delay)).isoformat(),isin,exchange,dataset,y)
                    print(exchange,code,dataset,y,state,detail)
    if errors: raise RuntimeError(f'{errors} source failures across {processed} jobs; persisted')
    if processed and not db.run("SELECT 1 FROM v2_financial_data LIMIT 1"): raise RuntimeError('No validated financial facts available; inspect coverage report')

def report(db):
    out={}
    for t in ['companies','facts','filings','v2_filings','v2_facts','v2_listings']: out[t]=int(db.run('SELECT COUNT(*) FROM '+t)[0][0])
    out['jobs']=db.run('SELECT source,dataset,state,COUNT(*) FROM v2_jobs GROUP BY source,dataset,state'); out['quality']=db.run('SELECT status,COUNT(*) FROM v2_facts GROUP BY status'); out['coverage']=db.run('SELECT d.isin,d.source,COUNT(*),MIN(f.end),MAX(f.end) FROM v2_facts f JOIN v2_filings d ON d.id=f.filing WHERE d.status=\'parsed\' GROUP BY d.isin,d.source'); out['verified']=0
    out['audit']=audit(db)
    os.makedirs('reports',exist_ok=True)
    with open('reports/coverage.json','w') as f: json.dump(out,f,indent=2)
    with open('reports/coverage.csv','w') as f:
        w=csv.writer(f); w.writerow(['isin','source','facts','earliest','latest']); w.writerows(out['coverage'])
    print(json.dumps({k:v for k,v in out.items() if k!='coverage'},indent=2)); print('Independent verification not performed; parsed facts are not certified.')
    return out

def main():
    p=argparse.ArgumentParser(); p.add_argument('command',choices=['init','refresh','work','report','export']); p.add_argument('--local'); p.add_argument('--limit',type=int,default=60); p.add_argument('--isin'); p.add_argument('--as-of'); a=p.parse_args(); db=DB(a.local); init(db)
    if a.command=='refresh': refresh(db,Source())
    elif a.command=='work': work(db,Source(),a.limit)
    elif a.command=='report': report(db)
    elif a.command=='export':
        if not a.isin: p.error('--isin required')
        sql='SELECT * FROM v2_financial_data WHERE isin=?'; args=[a.isin]
        if a.as_of: sql+=' AND published IS NOT NULL AND published<=?'; args.append(a.as_of)
        print(json.dumps(db.run(sql,*args)))
if __name__=='__main__': main()
