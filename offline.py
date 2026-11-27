"""Replay captured NSE evidence without network or Turso access."""
import argparse,csv,gzip,hashlib,json,os,zipfile
from pathlib import Path
from collections import Counter
from main import WATCH,load
from database import DB
from nse_extractor import ET,X,local

def replay(evidence,out):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    os.environ.pop('TURSO_DATABASE_URL',None);os.environ.pop('TURSO_AUTH_TOKEN',None)
    os.environ['LOCAL_DB']=str((out/'pilot.sqlite').resolve())
    db=DB();db.init(); docs={}; rows={}; annual=[]; errors=[]; inventory=[]
    with zipfile.ZipFile(evidence) as z:
        for name in z.namelist():
            if not name.startswith('archive/') or not name.endswith('.gz'):continue
            raw=gzip.decompress(z.read(name)); sha=hashlib.sha256(raw).hexdigest()
            if Path(name).stem!=sha:raise ValueError('Archive checksum mismatch: '+name)
            if raw.lstrip().startswith((b'{',b'[')):
                obj=json.loads(raw); records=obj if isinstance(obj,list) else obj.get('data',[])
                for row in records:
                    if not isinstance(row,dict):continue
                    if row.get('symbol') in WATCH and row.get('xbrl'):rows[row['xbrl']]=row
                    if row.get('fileName'):annual.append(row)
            elif b'<xbrli:xbrl' in raw[:3000]:docs[sha]=raw
    # Match XML identity, scope and reporting date to metadata; never guess ambiguous links.
    for sha,raw in docs.items():
        root=ET.fromstring(raw)
        meta={local(e.tag):e.text for e in root if local(e.tag) in ('Symbol','ISIN','ScripCode','NatureOfReportStandaloneConsolidated','DateOfEndOfReportingPeriod','DateOfBoardMeetingWhenResultsWereApproved')}
        symbol=meta.get('Symbol'); matches=[]
        for url,row in rows.items():
            if row['symbol']!=symbol:continue
            # Archive XML filenames are not retained; match by exact document metadata below.
            matches.append((url,row))
        inventory.append({'sha':sha,'symbol':symbol,'metadata':meta,'candidate_urls':len(matches)})
        try:
            # Use content-addressed archive URI unless exact URL association is established.
            scopes={e.text for e in root if local(e.tag) in ('NatureOfReportStandaloneConsolidated','WhetherResultsAreConsolidatedOrStandalone','ConsolidatedOrStandalone','TypeOfFinancialResults')}
            from nse_extractor import scope
            sc={scope(s) for s in scopes}-{'UNKNOWN'}
            if len(sc)!=1:raise ValueError('Unresolved document scope: '+str(sc))
            n,q=load(db,symbol,'offline_archive',{'consolidated':next(iter(sc))},'archive:sha256:'+sha,raw)
            inventory[-1].update(facts=n,issues=q)
        except Exception as e:errors.append({'sha':sha,'symbol':symbol,'error':str(e)})
    for view in ('observations','pnl_statement','balance_sheet','cash_flow','financial_revisions'):
        cur=db.local.execute('SELECT * FROM vw_fund_'+view); 
        with (out/(view+'.csv')).open('w',newline='') as f:
            w=csv.writer(f);w.writerow([d[0] for d in cur.description]);w.writerows(cur)
    summary={'xml_documents':len(docs),'metadata_xml_urls':len(rows),'annual_report_metadata':len(annual),'errors':errors,'coverage':db.run('SELECT symbol,statement,COUNT(*),MIN(period_end),MAX(period_end) FROM vw_fund_observations GROUP BY symbol,statement'),'checks':dict(Counter(json.loads(r[0])['status'] for r in db.run('SELECT result FROM ft_validation'))),'storage_bytes':db.storage(),'accepted':False}
    (out/'summary.json').write_text(json.dumps(summary,indent=2));(out/'inventory.json').write_text(json.dumps(inventory,indent=2));(out/'annual_report_metadata.json').write_text(json.dumps(annual,indent=2));print(json.dumps(summary,indent=2));return summary
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('evidence');p.add_argument('--out',default='offline_reports');a=p.parse_args();replay(a.evidence,a.out)
