"""Conservative audit: structural checks are not independent verification."""
import csv,json,os
from decimal import Decimal
FIN="('legacy','integrated','results')"
# Exact local names only; mappings are candidates, not taxonomy certification.
MAP={'RevenueFromOperations':'income','ProfitLoss':'income','ProfitLossForPeriod':'income','Assets':'balance','TotalAssets':'balance','EquityAndLiabilities':'balance','CashFlowsFromUsedInOperatingActivities':'cashflow','NetCashFlowsFromUsedInOperatingActivities':'cashflow'}
def write(name,header,rows):
    with open('reports/'+name+'.csv','w',newline='') as f:
        w=csv.writer(f); w.writerow(header); w.writerows(rows)
def audit(db):
    os.makedirs('reports',exist_ok=True)
    datasets=db.run('SELECT d.source,d.dataset,f.status,COUNT(*),COUNT(DISTINCT d.isin) FROM v2_facts f JOIN v2_filings d ON d.id=f.filing WHERE d.status=\'parsed\' GROUP BY 1,2,3')
    write('dataset_coverage',['source','dataset','status','facts','companies'],datasets)
    companies=db.run("SELECT c.isin,c.symbol,COUNT(f.id),MIN(f.end),MAX(f.end) FROM companies c LEFT JOIN v2_filings d ON d.isin=c.isin AND d.status='parsed' AND d.dataset IN "+FIN+" LEFT JOIN v2_facts f ON f.filing=d.id WHERE c.active=1 GROUP BY c.isin,c.symbol")
    write('company_coverage',['isin','symbol','financial_facts','earliest','latest'],companies)
    write('legacy_data_audit',['exchange','dataset','facts','companies'],db.run('SELECT d.exchange,d.dataset,COUNT(*),COUNT(DISTINCT d.isin) FROM facts f JOIN filings d ON d.id=f.filing GROUP BY 1,2'))
    write('source_health',['source','dataset','state','detail','jobs'],db.run('SELECT source,dataset,state,detail,COUNT(*) FROM v2_jobs GROUP BY 1,2,3,4'))
    write('quality_issues',['filing','rule','detail'],db.run('SELECT * FROM v2_issues'))
    # Aggregate candidates by period, never infer a complete statement from a single concept.
    rows=db.run('SELECT d.isin,d.source,f.start,f.end,f.scope,f.concept,COUNT(*) FROM v2_facts f JOIN v2_filings d ON d.id=f.filing WHERE d.status=\'parsed\' AND d.dataset IN '+FIN+' GROUP BY 1,2,3,4,5,6')
    counts={}
    for isin,source,start,end,scope,concept,n in rows:
        kind=MAP.get(concept.rsplit('}',1)[-1]);
        if kind:
            key=(isin,source,start,end,scope,kind); counts[key]=counts.get(key,0)+int(n)
    write('financial_statement_coverage',['isin','source','start','end','scope','candidate_statement','facts'],[(*k,v) for k,v in counts.items()])
    usage=storage(db)
    with open('reports/database_usage.json','w') as f: json.dump(usage,f,indent=2)
    return {'financial_companies':sum(int(r[2])>0 for r in companies),'active_companies':len(companies),'dataset_facts':datasets,'storage':usage,'independent_verification':'not performed'}
def storage(db):
    try:
        size=int(db.run('PRAGMA page_count')[0][0])*int(db.run('PRAGMA page_size')[0][0])
    except Exception: size=None
    limit=int(os.environ.get('DATABASE_BYTE_BUDGET','400000000'))
    return {'estimated_database_bytes':size,'configured_budget_bytes':limit,'fraction':size/limit if size is not None else None,'note':'Local database page estimate, not provider account quota; budget is conservative application setting.'}
