"""Public yfinance accessor diagnostics, canonical inventory and expected-cell audit."""
import argparse,csv,json,zipfile,hashlib,time,os,re
from pathlib import Path
from datetime import date
from collections import defaultdict
from yahoo_pilot import WATCH, ROUTES, extract, csvout, number
from yahoo_quality import MAPPING
CORE={'PNL':['Total Revenue','Operating Revenue','Pretax Income','Tax Provision','Net Income','Basic EPS','Diluted EPS'], 'BS':['Total Assets','Total Liabilities Net Minority Interest','Total Equity Gross Minority Interest','Cash And Cash Equivalents'], 'CF':['Operating Cash Flow','Investing Cash Flow','Financing Cash Flow','Beginning Cash Position','End Cash Position']}
EXTRA={'PNL':['Gross Profit','EBITDA','EBIT','Cost Of Revenue','Operating Income','Interest Expense','Reconciled Depreciation','Total Expenses'], 'BS':['Current Assets','Current Liabilities','Total Debt','Long Term Debt','Current Debt','Inventory','Net PPE'], 'CF':['Capital Expenditure','Free Cash Flow']}
BANK=['Interest Income','Interest Expense','Net Interest Income']
def code(label):return re.sub('[^A-Z0-9]+','_',label.upper()).strip('_')
def inventory(rows):
    labels={(s,m) for s,ms in MAPPING.items() for m in ms}|{(r['statement'],r['metric']) for r in rows}
    return [dict(statement=s,source_label=m,canonical_code=s+'_'+code(m),unit_kind=MAPPING.get(s,{}).get(m,'UNKNOWN')) for s,m in sorted(labels)]
def audit(rows):
    cells=defaultdict(set)
    for r in rows:cells[(r['symbol'],r['statement'],r['frequency'],r['period_end'],r['metric'])].add((r['unit'],r['value_raw']))
    result=[]
    for symbol in WATCH:
        for year in range(2022,2028):
            for freq,ends in [('ANNUAL',[f'{year}-03-31']),('QUARTERLY',[f'{year-1}-06-30',f'{year-1}-09-30',f'{year-1}-12-31',f'{year}-03-31'])]:
                for end in ends:
                    if end>date.today().isoformat():continue
                    for stmt in CORE:
                        # Indian interim BS/CF commonly half-yearly; no quarterly completeness mandate here.
                        if freq=='QUARTERLY' and stmt!='PNL':continue
                        metrics=CORE[stmt]+(BANK if symbol=='HDFCBANK' and stmt=='PNL' else EXTRA[stmt] if symbol!='HDFCBANK' else [])
                        for m in metrics:
                            v=cells.get((symbol,stmt,freq,end,m),set());result.append(dict(symbol=symbol,statement=stmt,frequency=freq,period_end=end,metric=m,status='CONFLICT' if len(v)>1 else 'AVAILABLE' if v else 'MISSING',expectation='CORE_CANDIDATE' if m in CORE[stmt] or m in BANK else 'OPTIONAL_CANDIDATE'))
    return result
def frame_cells(df):
    return {(str(df.index[i]),str(df.columns[j])[:10],v) for i in range(len(df.index)) for j in range(len(df.columns)) if (v:=number(df.iloc[i,j])) is not None}
def probe(factory):
    diagnostics=[]; trailing=[]; profiles=[]
    for symbol in WATCH:
        t=factory(symbol+'.NS'); info={}
        try:info=t.get_info();profiles.append(dict(symbol=symbol,profile=info))
        except Exception as e:diagnostics.append(dict(symbol=symbol,route='profile',status=type(e).__name__,facts=0,property_only=0,method_only=0,raw_labels=''))
        for route,(stmt,freq) in list(ROUTES.items())+[('ttm_income_stmt',('PNL','TTM')),('ttm_cashflow',('CF','TTM'))]:
            method={'PNL':'get_income_stmt','BS':'get_balance_sheet','CF':'get_cashflow'}[stmt]; f={'ANNUAL':'yearly','QUARTERLY':'quarterly','TTM':'trailing'}[freq]
            try:
                time.sleep(float(os.getenv('YAHOO_DELAY','1.2'))); a=frame_cells(getattr(t,route)); b=frame_cells(getattr(t,method)(pretty=True,freq=f)); raw=getattr(t,method)(pretty=False,freq=f)
                diagnostics.append(dict(symbol=symbol,route=route,status='OK' if a or b else 'EMPTY',facts=len(a|b),property_only=len(a-b),method_only=len(b-a),raw_labels='|'.join(map(str,raw.index))))
                if freq=='TTM':
                    for label,end,value in sorted(a|b):trailing.append(dict(symbol=symbol,statement=stmt,frequency=freq,period_end=end,metric=label,value_raw=value,currency=info.get('financialCurrency','UNKNOWN')))
            except Exception as e:diagnostics.append(dict(symbol=symbol,route=route,status=type(e).__name__,facts=0,property_only=0,method_only=0,raw_labels=''))
    return diagnostics,trailing,profiles
def main():
    p=argparse.ArgumentParser();p.add_argument('--replay',type=Path);p.add_argument('--out',type=Path,default=Path('reports/yahoo'));a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    if a.replay:
        with zipfile.ZipFile(a.replay) as z:
            raw=z.read('snapshot.json');meta=json.loads(z.read('summary.json'))
            if hashlib.sha256(raw).hexdigest()!=meta['sha256']:raise ValueError('checksum mismatch')
            rows=json.loads(raw)
    else:rows=json.loads((a.out/'snapshot.json').read_text())
    gaps=audit(rows); inv=inventory(rows);csvout(a.out/'expected_cells.csv',gaps);csvout(a.out/'canonical_inventory.csv',inv)
    summary={'facts':len(rows),'inventory':len(inv),'expected_cells':len(gaps),'missing':sum(r['status']=='MISSING' for r in gaps),'live_probe':False}
    if not a.replay:
        import yfinance as yf
        d,t,p=probe(yf.Ticker);csvout(a.out/'accessor_diagnostics.csv',d);csvout(a.out/'trailing_financials.csv',t);(a.out/'company_profiles.json').write_text(json.dumps(p,default=str));summary.update(live_probe=True,yfinance_version=yf.__version__,routes=len(d),trailing_facts=len(t))
    (a.out/'deep_summary.json').write_text(json.dumps(summary,indent=2))
if __name__=='__main__':main()
