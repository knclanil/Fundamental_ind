"""Additive quality layer. Original observations and source units are immutable."""
import json
from pathlib import Path
from decimal import Decimal
from datetime import date
from collections import defaultdict
MAPPING=json.loads(Path(__file__).with_name('yahoo_metric_units.json').read_text())
def classify(rows,coverage):
    currencies={c['symbol']:c['currency'] for c in coverage if c.get('currency') not in ('UNKNOWN',None,'')}
    result=[]
    for r in rows:
        kind=MAPPING.get(r['statement'],{}).get(r['metric'],'UNKNOWN'); currency=currencies.get(r['symbol'],'UNKNOWN')
        u=currency if kind=='MONEY' else currency+'/share' if kind=='PER_SHARE' and currency!='UNKNOWN' else kind if kind in ('SHARES','RATIO') else 'UNKNOWN'
        result.append(dict(r,classified_unit=u,unit_kind=kind,currency_basis='PROVIDER_METADATA_UNVERIFIED'))
    return result

def validate(rows):
    groups=defaultdict(lambda:defaultdict(set))
    for r in rows:
        if r['unit_kind']=='MONEY':groups[(r['symbol'],r['statement'],r['frequency'],r['period_end'],r['classified_unit'])][r['metric']].add(Decimal(r['value_raw']))
    result=[]
    for key,g in sorted(groups.items()):
        equations={'BS':[('balance',{'Total Assets':1,'Total Liabilities Net Minority Interest':-1,'Total Equity Gross Minority Interest':-1})], 'PNL':[('tax',{'Pretax Income':1,'Tax Provision':-1,'Net Income Including Noncontrolling Interests':-1})], 'CF':[('cash_movement',{'Operating Cash Flow':1,'Investing Cash Flow':1,'Financing Cash Flow':1,'Changes In Cash':-1}),('cash_bridge',{'End Cash Position':1,'Beginning Cash Position':-1,'Changes In Cash':-1,'Effect Of Exchange Rate Changes':-1})]}[key[1]]
        for name,terms in equations:
            if name=='cash_bridge' and 'Other Cash Adjustment Outside Changein Cash' in g:terms['Other Cash Adjustment Outside Changein Cash']=-1
            missing=[m for m in terms if len(g.get(m,()))!=1]; delta=None; tol=None
            if not missing and key[-1]!='UNKNOWN':
                vals=[next(iter(g[m])) for m in terms];delta=sum(v*c for v,c in zip(vals,terms.values()));tol=max(Decimal(1),max(map(abs,vals))*Decimal('0.000001'))
            result.append(dict(zip(('symbol','statement','frequency','period_end','currency'),key),check=name,status='NOT_EVALUATED' if delta is None else 'PASS' if abs(delta)<=tol else 'REVIEW',delta='' if delta is None else str(delta),tolerance='' if tol is None else str(tol),missing='|'.join(missing)))
    return result

def freshness(rows, today=None):
    today=today or date.today(); month=((today.month-1)//3)*3+1; start=date(today.year,month,1)
    from datetime import timedelta
    expected=start-timedelta(days=1)
    if (today-expected).days<60:expected=date(expected.year,expected.month-2,1)-timedelta(days=1)
    result=[]
    for s in sorted({r['symbol'] for r in rows}):
        dates=[r['period_end'] for r in rows if r['symbol']==s and r['statement']=='PNL' and r['frequency']=='QUARTERLY']; latest=max(dates,default='')
        result.append(dict(symbol=s,latest=latest,expected=expected.isoformat(),status='PASS' if latest>=expected.isoformat() else 'STALE'))
    return result

def persist(db,rows,validation,fresh,stamp,sha):
    db.run('CREATE TABLE IF NOT EXISTS dm_yahoo_unit_mapping(statement TEXT,metric TEXT,kind TEXT,PRIMARY KEY(statement,metric))')
    db.run('CREATE TABLE IF NOT EXISTS misc_yahoo_quality_snapshot(snapshot_sha TEXT PRIMARY KEY,retrieved TEXT,report TEXT)')
    db.run('CREATE TABLE IF NOT EXISTS ft_yahoo_classification(fact_id INTEGER,snapshot_sha TEXT,retrieved TEXT,classified_unit TEXT,unit_kind TEXT,PRIMARY KEY(fact_id,snapshot_sha,retrieved))')
    for s,ms in MAPPING.items():
        for m,k in ms.items():db.run('INSERT OR IGNORE INTO dm_yahoo_unit_mapping VALUES(?,?,?)',(s,m,k))
    for start in range(0,len(rows),100):
        batch=rows[start:start+100]; args=tuple(v for r in batch for v in (r['symbol'],r['statement'],r['metric'],r['frequency'],r['period_end'],r['unit'],r['value_raw'],r['classified_unit'],r['unit_kind']))
        db.run('WITH incoming(symbol,statement,metric,frequency,period_end,unit,value_raw,classified_unit,kind) AS (VALUES '+','.join(['(?,?,?,?,?,?,?,?,?)']*len(batch))+') INSERT OR IGNORE INTO ft_yahoo_classification SELECT f.id,?,?,i.classified_unit,i.kind FROM incoming i JOIN dm_yahoo_company c ON c.symbol=i.symbol JOIN dm_yahoo_metric m ON m.statement=i.statement AND m.label=i.metric JOIN dm_yahoo_period p ON p.frequency=i.frequency AND p.period_end=i.period_end JOIN ft_yahoo_financials f ON f.company_id=c.id AND f.metric_id=m.id AND f.period_id=p.id AND f.unit=i.unit AND f.value_raw=i.value_raw',args+(sha,stamp))
    count=int(db.run('SELECT count(*) FROM ft_yahoo_classification WHERE snapshot_sha=? AND retrieved=?',(sha,stamp))[0][0]); assert count==len(rows), (count,len(rows))
    db.run('INSERT OR REPLACE INTO misc_yahoo_quality_snapshot VALUES(?,?,?)',(sha,stamp,json.dumps({'validation':validation,'freshness':fresh,'expansion_allowed':False})))
    db.run('''CREATE VIEW IF NOT EXISTS vw_fund_yahoo_classified AS SELECT o.*,q.classified_unit,q.unit_kind,q.snapshot_sha,q.retrieved,CASE WHEN q.classified_unit='INR' THEN CAST(o.value_raw AS REAL)/10000000.0 END classified_crores,CASE WHEN q.classified_unit='USD' THEN CAST(o.value_raw AS REAL)/1000000.0 END usd_millions FROM vw_fund_yahoo_observations o JOIN dm_yahoo_company c ON c.symbol=o.symbol JOIN dm_yahoo_metric m ON m.statement=o.statement AND m.label=o.metric JOIN dm_yahoo_period p ON p.frequency=o.frequency AND p.period_end=o.period_end JOIN ft_yahoo_financials f ON f.company_id=c.id AND f.metric_id=m.id AND f.period_id=p.id AND f.unit=o.unit AND f.value_raw=o.value_raw JOIN ft_yahoo_classification q ON q.fact_id=f.id''')
    return count
