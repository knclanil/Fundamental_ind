"""Fail-closed pilot checks; no inferred verification from matching labels alone."""
import os, datetime as dt, json, csv
from decimal import Decimal, InvalidOperation
from collections import defaultdict
from quality import write, storage
SYMBOLS=('RELIANCE','INFY','HDFCBANK','TATASTEEL','HINDUNILVR')

def bounds(today=None):
    today=today or dt.date.today(); fy=today.year if today.month>=4 else today.year-1
    return dt.date(fy-5,4,1),today

def init(db):
    db.run('CREATE TABLE IF NOT EXISTS pilot_members(symbol TEXT PRIMARY KEY)')
    for s in SYMBOLS: db.run('INSERT OR IGNORE INTO pilot_members VALUES(?)',s)

def tolerance(decimals):
    if decimals=='INF': return Decimal(0)
    try:
        n=int(decimals)
        if abs(n)>30: return None
        return Decimal(10)**(-n)/2
    except (ValueError,TypeError): return None

def validate(db):
    os.makedirs('reports',exist_ok=True)
    start,end=bounds(); checks=[]; coverage=[]; matches=[]; missing=[]
    members=db.run('SELECT p.symbol,c.isin FROM pilot_members p LEFT JOIN companies c ON c.symbol=p.symbol AND c.active=1 ORDER BY p.symbol')
    for symbol,isin in members:
        rows=db.run("SELECT d.id,d.source,d.sha,f.concept,f.start,f.end,f.scope,f.unit,f.value,f.decimals FROM v2_facts f JOIN v2_filings d ON d.id=f.filing WHERE d.isin=? AND d.status='parsed' AND d.dataset IN ('legacy','integrated','results') AND f.status='validated' AND f.dimensions='[]' AND f.end>=? AND f.end<=?",isin,start.isoformat(),end.isoformat()) if isin else []
        groups=defaultdict(dict); cross=defaultdict(list); periods=set()
        for fid,source,sha,concept,a,b,scope,unit,value,dec in rows:
            try: v=Decimal(value); assert v.is_finite()
            except (InvalidOperation,TypeError,AssertionError): continue
            t=tolerance(dec); name=concept.rsplit('}',1)[-1]
            if scope not in ('standalone','consolidated') or not unit: continue
            groups[(fid,a,b,scope,unit)].setdefault(name,set()).add((v,t))
            if a and name in ('RevenueFromOperations','ProfitLoss','ProfitLossForPeriod'):
                try:
                    days=(dt.date.fromisoformat(b)-dt.date.fromisoformat(a)).days+1
                    if 80<=days<=100: periods.add((b,scope))
                except ValueError: pass
            cross[(concept,a,b,scope,unit)].append((source,sha,v,t))
        for key,values in groups.items():
            for rule,names in [('balance',('Assets','Liabilities','Equity')),('tax',('ProfitBeforeTax','TaxExpense','ProfitLoss'))]:
                if not all(len(values.get(n,()))==1 for n in names): continue
                x,y,z=[next(iter(values[n])) for n in names]; diff=x[0]-y[0]-z[0]; ts=[x[1],y[1],z[1]]
                result='unknown_precision' if None in ts else 'pass' if abs(diff)<=sum(ts) else 'fail'
                checks.append((symbol,*key,rule,str(diff),result))
        for key,vals in cross.items():
            nse=[v for v in vals if v[0]=='NSE']; bse=[v for v in vals if v[0]=='BSE']
            if not nse or not bse: continue
            # No version equivalence is established: these are candidates only.
            for x in nse:
                for y in bse:
                    if x[2]==y[2]: matches.append((symbol,*key,x[1],y[1],str(x[2]),'candidate_equal'))
                    else: matches.append((symbol,*key,x[1],y[1],str(x[2]-y[2]),'candidate_difference'))
        for year in range(start.year,end.year+1):
            for month,day in ((3,31),(6,30),(9,30),(12,31)):
                d=dt.date(year,month,day)
                if start<=d<=end:
                    for scope in ('standalone','consolidated'):
                        if (d.isoformat(),scope) not in periods: missing.append((symbol,d.isoformat(),scope,'missing_quarter_candidate'))
        coverage.append((symbol,isin,len(rows),len(periods),0))
    write('company_quality_score',['symbol','isin','structural_facts','quarter_candidates','verified'],coverage)
    write('accounting_checks',['symbol','filing','start','end','scope','unit','rule','difference','result'],checks)
    write('financial_reconciliation',['symbol','concept','start','end','scope','unit','nse_sha','bse_sha','value_or_difference','result'],matches)
    write('missing_periods',['symbol','end','scope','reason'],missing)
    try: sizes=db.run('SELECT name,SUM(pgsize) FROM dbstat GROUP BY name ORDER BY SUM(pgsize) DESC')
    except Exception: sizes=[]
    write('table_storage',['object','bytes'],sizes)
    result={'accepted':False,'expansion_enabled':False,'verified_facts':0,'start':str(start),'end':str(end),'members':len(members),'accounting_checks':len(checks),'accounting_failures':sum(r[-1]=='fail' for r in checks),'missing_quarter_candidates':len(missing),'storage':storage(db),'blockers':['Independent official PDF verification not implemented','Filing version equivalence and taxonomy certification not established','Backup restoration and capacity scalability not demonstrated']}
    for name in ('pilot_acceptance','validation_summary'):
        with open('reports/'+name+'.json','w') as f: json.dump(result,f,indent=2)
    return result
