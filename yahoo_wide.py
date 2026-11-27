"""Read-only analytical layer; no currency translation or fact duplication."""
import re
from yahoo_quality import MAPPING

KEY='symbol,frequency,period_end'
ALIASES={'Total Revenue':'revenue','Basic EPS':'eps','Pretax Income':'pbt','Total Liabilities Net Minority Interest':'total_liabilities','Total Equity Gross Minority Interest':'total_equity'}
def quote(s):return '"'+s.replace('"','""')+'"'
def literal(s):return "'"+s.replace("'","''")+"'"
def install(db):
    views={}
    views['latest'] = '''WITH ranked AS (SELECT *,DENSE_RANK() OVER(PARTITION BY symbol,statement,frequency,period_end,metric ORDER BY retrieved DESC,snapshot_sha DESC) AS snapshot_rank FROM vw_fund_yahoo_classified) SELECT * FROM ranked WHERE snapshot_rank=1'''
    views['cells']='''SELECT symbol,statement,frequency,period_end,metric,MIN(classified_unit) AS unit,COUNT(DISTINCT classified_unit || ':' || value_raw) AS variants,CASE WHEN COUNT(DISTINCT classified_unit || ':' || value_raw)=1 THEN MAX(CASE classified_unit WHEN 'INR' THEN classified_crores WHEN 'USD' THEN usd_millions WHEN 'UNKNOWN' THEN NULL ELSE CAST(value_raw AS REAL) END) END AS display_value FROM vw_fund_yahoo_latest GROUP BY symbol,statement,frequency,period_end,metric'''
    views['completeness']='''SELECT symbol,statement,frequency,period_end,COUNT(*) AS observed_metrics,SUM(CASE WHEN variants>1 THEN 1 ELSE 0 END) AS conflicting_metrics,SUM(CASE WHEN display_value IS NULL THEN 1 ELSE 0 END) AS unavailable_metrics FROM vw_fund_yahoo_cells GROUP BY symbol,statement,frequency,period_end'''
    columns={}
    for statement,short in [('PNL','pnl'),('BS','bs'),('CF','cf')]:
        labels=set(MAPPING[statement])|{r[0] for r in db.run('SELECT label FROM dm_yahoo_metric WHERE statement=?',(statement,))}
        ordered=sorted(labels,key=lambda m:(m not in ALIASES,m)); used=set(); expr=[]; columns[short]=[]
        for m in ordered:
            base=ALIASES.get(m,re.sub(r'[^a-z0-9]+','_',m.lower()).strip('_')) or 'metric'; name=base; n=2
            while name in used:name=f'{base}_{n}';n+=1
            used.add(name);columns[short].append(name)
            expr.append(f'MAX(CASE WHEN metric={literal(m)} THEN display_value END) AS {quote(name)}')
        views[short+'_wide']=f'''SELECT {KEY},'FY'||(CAST(substr(period_end,1,4) AS INTEGER)+CASE WHEN substr(period_end,6,2)>'03' THEN 1 ELSE 0 END) AS financial_year,CASE WHEN frequency='QUARTERLY' THEN ((CAST(substr(period_end,6,2) AS INTEGER)+8)%12)/3+1 END AS financial_quarter,CASE WHEN COUNT(DISTINCT CASE WHEN unit IN ('INR','INR/share') THEN 'INR' WHEN unit IN ('USD','USD/share') THEN 'USD' END)=1 THEN MAX(CASE WHEN unit IN ('INR','INR/share') THEN 'INR' WHEN unit IN ('USD','USD/share') THEN 'USD' END) ELSE 'UNKNOWN_OR_MIXED' END AS currency,SUM(CASE WHEN variants>1 THEN 1 ELSE 0 END) AS conflicting_metrics,'UNVERIFIED' AS quality_status,{','.join(expr)} FROM vw_fund_yahoo_cells WHERE statement='{statement}' GROUP BY {KEY}'''
        for freq in ('annual','quarterly'):views[short+'_'+freq]=f"SELECT * FROM vw_fund_yahoo_{short}_wide WHERE frequency='{freq.upper()}'"
    views['financial_summary']='''WITH keys AS (SELECT symbol,frequency,period_end FROM vw_fund_yahoo_pnl_wide UNION SELECT symbol,frequency,period_end FROM vw_fund_yahoo_bs_wide UNION SELECT symbol,frequency,period_end FROM vw_fund_yahoo_cf_wide) SELECT k.*,p.currency AS pnl_currency,b.currency AS bs_currency,c.currency AS cf_currency,COALESCE(p.financial_year,b.financial_year,c.financial_year) AS financial_year,COALESCE(p.financial_quarter,b.financial_quarter,c.financial_quarter) AS financial_quarter,p.revenue,p.ebitda,p.ebit,p.pbt,p.net_income,p.eps,b.total_assets,b.total_liabilities,b.total_equity,b.total_debt,c.operating_cash_flow,c.investing_cash_flow,c.financing_cash_flow,c.free_cash_flow,'UNVERIFIED' AS quality_status FROM keys k LEFT JOIN vw_fund_yahoo_pnl_wide p USING(symbol,frequency,period_end) LEFT JOIN vw_fund_yahoo_bs_wide b USING(symbol,frequency,period_end) LEFT JOIN vw_fund_yahoo_cf_wide c USING(symbol,frequency,period_end)'''
    for freq in ('annual','quarterly'):views['financial_'+freq]=f"SELECT * FROM vw_fund_yahoo_financial_summary WHERE frequency='{freq.upper()}'"
    views['ratios']='''SELECT *,CASE WHEN pnl_currency IN ('INR','USD') AND revenue>0 THEN 100.0*net_income/revenue END AS net_profit_margin,CASE WHEN pnl_currency IN ('INR','USD') AND revenue>0 THEN 100.0*ebitda/revenue END AS ebitda_margin,CASE WHEN bs_currency IN ('INR','USD') AND total_equity>0 THEN total_debt/total_equity END AS debt_to_equity FROM vw_fund_yahoo_financial_summary'''
    views['growth']='''SELECT a.*,CASE WHEN a.pnl_currency=b.pnl_currency AND a.pnl_currency IN ('INR','USD') AND b.revenue>0 THEN 100.0*(a.revenue/b.revenue-1) END AS revenue_yoy_pct FROM vw_fund_yahoo_financial_summary a LEFT JOIN vw_fund_yahoo_financial_summary b ON a.symbol=b.symbol AND a.frequency=b.frequency AND substr(a.period_end,6)=substr(b.period_end,6) AND CAST(substr(a.period_end,1,4) AS INTEGER)=CAST(substr(b.period_end,1,4) AS INTEGER)+1'''
    for name,sql in views.items():
        db.run('DROP VIEW IF EXISTS vw_fund_yahoo_'+name);db.run('CREATE VIEW vw_fund_yahoo_'+name+' AS '+sql)
    return {name:int(db.run('SELECT COUNT(*) FROM vw_fund_yahoo_'+name)[0][0]) for name in views}
