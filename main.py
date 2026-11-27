import argparse,json,hashlib,gzip,os
from pathlib import Path
from datetime import date,datetime
from database import DB
from nse_extractor import NSE,parse,scope,ET,local
from quality_checks import checks
ROOT=Path(__file__).parent
WATCH=json.loads((ROOT/'watchlist.json').read_text())
def archive(raw):
    sha=hashlib.sha256(raw).hexdigest(); p=Path('archive')/(sha+'.gz'); p.parent.mkdir(exist_ok=True)
    if not p.exists():p.write_bytes(gzip.compress(raw))
    return sha
def published(row):
    for key in ('filingDate','broadcastDate','broadcast_Date','broadCastDate'):
        v=row.get(key)
        if v:
            for fmt in ('%d-%b-%Y %H:%M:%S','%d-%b-%Y','%d-%m-%Y %H:%M:%S','%Y-%m-%d %H:%M:%S','%Y-%m-%d'):
                try:return datetime.strptime(str(v),fmt).isoformat()
                except ValueError:pass
    return None
def load(db,symbol,route,row,url,raw):
    sha=archive(raw); facts,issues=parse(raw,scope(row.get('consolidated') or row.get('consolidation') or row.get('resultType'))); db.guard()
    isin,sector=WATCH[symbol]; root=ET.fromstring(raw); declared={e.text.strip() for e in root if local(e.tag)=='ISIN' and e.text}; declared_symbols={e.text.strip() for e in root if local(e.tag)=='Symbol' and e.text}; codes={'RELIANCE':'500325','INFY':'500209','HDFCBANK':'500180','TATASTEEL':'500470','HINDUNILVR':'500696'}; cid=db.id('dm_company',symbol=symbol,isin=isin,sector=sector)
    db.run('INSERT OR IGNORE INTO dm_filing(company_id,url,sha,published,seq,endpoint) VALUES(?,?,?,?,?,?)',(cid,url,sha,published(row),str(row.get('seqNumber') or row.get('seq_Id') or ''),route)); fid=int(db.run('SELECT id FROM dm_filing WHERE company_id=? AND url=? AND sha=?',(cid,url,sha))[0][0]); n=0
    for f in facts:
        if f['entity']!=isin and not (declared=={isin} and declared_symbols=={symbol} and f['entity'] in (symbol,codes[symbol])): f['status']='UNMAPPED_OR_AMBIGUOUS'
        if f['status']!='CANDIDATE':issues.append(f);continue
        if n%100==0:db.guard()
        mid=db.id('dm_metric',code=f['code'],statement=f['statement']); pid=db.id('dm_period',start=f['start'],end=f['end'],kind=f['period']); xid=db.id('dm_context',scope=f['scope'],dimensions=f['dimensions']); uid=db.id('dm_unit',name=f['unit']); fact=db.id('ft_financials',company_id=cid,metric_id=mid,period_id=pid,context_id=xid,unit_id=uid,value_raw=f['value'],status='CANDIDATE'); db.run('INSERT OR IGNORE INTO ft_source_reference VALUES(?,?,?,?)',(fact,fid,f['tag'],f['decimals'])); n+=1
    for result in checks(facts):db.run('INSERT OR IGNORE INTO ft_validation VALUES(?,?)',(fid,json.dumps(result,sort_keys=True)))
    # Keep bulky rejected facts outside Turso.
    if issues:archive(json.dumps(issues).encode())
    return n,len(issues)
def main():
    p=argparse.ArgumentParser(); p.add_argument('--init-db',action='store_true');p.add_argument('--source-audit',action='store_true');p.add_argument('--report',action='store_true');p.add_argument('--symbol',choices=WATCH);p.add_argument('--watchlist',default='watchlist.json');p.add_argument('--backfill-years',type=int,default=5);p.add_argument('--max-jobs',type=int,default=10);a=p.parse_args(); db=None if a.source_audit else DB(); reports=Path('reports');reports.mkdir(exist_ok=True)
    if db:db.init()
    if a.init_db:return
    if a.report:
        print(json.dumps({'storage_bytes':db.storage(),'coverage':db.run('SELECT symbol,statement,COUNT(*),MIN(period_end),MAX(period_end) FROM vw_fund_observations GROUP BY symbol,statement'),'pilot_accepted':False,'blocker':'Independent verification not implemented'}));return
    client=NSE(); log=[]; today=date.today(); fy=today.year-(today.month<4); symbols=[a.symbol] if a.symbol else list(WATCH); jobs=0
    try:
        # Direct documented website API discovery; no mandatory homepage request.
        # Stop this source on access denial; never switch hosts to evade a denial.
        for year in range(today.year,fy-a.backfill_years-1,-1):
            for symbol in symbols:
                for route in ('integrated-filing-results','corporates-financial-results','annual-reports'):
                    if route=='integrated-filing-results' and year<2024:continue
                    if route=='annual-reports' and year!=today.year:continue
                    if jobs>=a.max_jobs:break
                    if client.blocked:break
                    if db:
                        old=db.run('SELECT state FROM misc_etl_jobs WHERE symbol=? AND year=? AND route=?',(symbol,year,route))
                        if old and old[0][0]=='parsed':continue
                    jobs+=1; rec={'symbol':symbol,'year':year,'route':route,'rows':0,'facts':0,'errors':[],'pdf_links':[],'documents':0}; log.append(rec)
                    try:
                        for raw,rows,params in client.discover(symbol,year,route):
                            archive(raw);rec['rows']+=len(rows);rec['params']=params
                            for row in rows:
                                if not isinstance(row,dict):continue
                                if route=='annual-reports':
                                    if isinstance(row.get('fileName'),str):rec['pdf_links'].append(row['fileName'])
                                    continue
                                if str(row.get('symbol','')).upper()!=symbol:continue
                                urls=set()
                                for k,v in row.items():
                                    if isinstance(v,str) and v.startswith('https://') and (v.lower().split('?')[0].endswith('.xml')):urls.add(v)
                                    elif isinstance(v,str) and v.startswith('https://') and v.lower().split('?')[0].endswith('.pdf'):rec['pdf_links'].append(v)
                                for url in urls:
                                    try:
                                        if client.blocked:break
                                        doc=client.get(url);archive(doc);rec['documents']+=1
                                        if db:n,q=load(db,symbol,route,row,url,doc);rec['facts']+=n
                                        else:fs,issues=parse(doc,scope(row.get('consolidated') or row.get('consolidation')));rec['facts']+=len(fs)
                                    except Exception as e:rec['errors'].append(str(e))
                    except Exception as e:rec['errors'].append(str(e))
                    state='blocked' if client.blocked else 'partial' if rec['errors'] else 'parsed' if rec['facts'] else 'empty'
                    if db:db.run('INSERT INTO misc_etl_jobs VALUES(?,?,?,?,?) ON CONFLICT(symbol,year,route) DO UPDATE SET state=excluded.state,detail=excluded.detail',(symbol,year,route,state,json.dumps({k:v for k,v in rec.items() if k!='pdf_links'})))
                    if client.blocked:break
    finally:
        (reports/'api_diagnostics.json').write_text(json.dumps(client.diagnostics,indent=2));(reports/'ingestion_summary.json').write_text(json.dumps({'status':'blocked' if client.blocked else 'acquired' if any(r['facts'] for r in log) else 'no_facts','jobs':len(log),'facts':sum(r['facts'] for r in log)},indent=2));(reports/'source_audit.json').write_text(json.dumps(log,indent=2));(reports/'pilot_acceptance.json').write_text(json.dumps({'accepted':False,'expansion_enabled':False,'blocker':'Independent verification and completeness not established'}))
    if not any(r['facts'] for r in log):
        print('No financial facts acquired; inspect reports/ingestion_summary.json and source_audit.json')
        return 2
    return 0
if __name__=='__main__':raise SystemExit(main())
