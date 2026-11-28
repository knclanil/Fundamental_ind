"""Additive current-observation analytics; no historical valuation inference."""
import json,csv,os
from pathlib import Path
from database import DB
from membership import init
METRICS={'revenue':('PNL','Total Revenue'),'net_income':('PNL','Net Income'),'gross_profit':('PNL','Gross Profit'),'ebitda':('PNL','EBITDA'),'ebit':('PNL','EBIT'),'operating_profit':('PNL','Operating Income'),'assets':('BS','Total Assets'),'equity':('BS','Stockholders Equity'),'debt':('BS','Total Debt'),'current_assets':('BS','Current Assets'),'current_liabilities':('BS','Current Liabilities'),'cash':('BS','Cash And Cash Equivalents'),'ocf':('CF','Operating Cash Flow'),'fcf':('CF','Free Cash Flow'),'capex':('CF','Capital Expenditure'),'pretax_income':('PNL','Pretax Income'),'basic_eps':('PNL','Basic EPS'),'diluted_eps':('PNL','Diluted EPS'),'investing_cash_flow':('CF','Investing Cash Flow'),'financing_cash_flow':('CF','Financing Cash Flow')}
def replace_view(db,sql):
 name=sql.split()[5]
 existing=db.run("SELECT sql FROM sqlite_schema WHERE type='view' AND name=?",(name,))
 if existing and existing[0][0].replace(' IF NOT EXISTS','').strip()==sql.replace(' IF NOT EXISTS','').strip():return
 db.run('DROP VIEW IF EXISTS '+name)
 db.run(sql)
def install(db):
 init(db)
 cols=','.join(f"""MAX(CASE WHEN statement='{s}' AND metric='{m}' AND unit='{"INR/share" if k.endswith("eps") else "INR"}' THEN display_value END) {k}""" for k,(s,m) in METRICS.items())
 replace_view(db,'CREATE VIEW IF NOT EXISTS vw_company_financial_base AS SELECT c.id company_id,c.symbol,f.frequency,f.period_end,'+cols+' FROM vw_fund_cells f JOIN dm_company c ON c.symbol=f.symbol GROUP BY c.id,c.symbol,f.frequency,f.period_end')
 replace_view(db,"CREATE VIEW IF NOT EXISTS vw_company_financial_statements AS SELECT *, 'UNKNOWN' reporting_basis, 'PUBLISHED_UNVERIFIED' quality_status FROM vw_company_financial_base")
 replace_view(db,"CREATE VIEW IF NOT EXISTS vw_missing_financial_periods AS WITH years(period_end) AS (VALUES ('2022-03-31'),('2023-03-31'),('2024-03-31'),('2025-03-31'),('2026-03-31')) SELECT c.id company_id,c.symbol,y.period_end FROM dm_company c CROSS JOIN years y LEFT JOIN vw_company_financial_base f ON f.company_id=c.id AND f.period_end=y.period_end AND f.frequency='ANNUAL' WHERE c.active=1 AND (f.revenue IS NULL OR f.net_income IS NULL OR f.assets IS NULL OR f.ocf IS NULL)")
 for sql in Path(__file__).with_name('intelligence.sql').read_text().split(';'):
  if sql.strip():replace_view(db,sql.strip())
 for category,source,key in [('sector','vw_sector_constituents','index_name'),('factor','vw_factor_constituents','factor'),('index','vw_index_constituents','index_name'),('industry','vw_company_industry_current','industry')]:
  replace_view(db,f'''CREATE VIEW IF NOT EXISTS vw_{category}_financial_summary AS WITH members AS (SELECT DISTINCT company_id,{key} group_name FROM {source}), periods AS (SELECT DISTINCT period_end FROM vw_company_financial_base WHERE frequency='ANNUAL') SELECT group_name,p.period_end,count(*) constituent_count,count(f.revenue) revenue_coverage,count(f.net_income) earnings_coverage,sum(f.revenue) revenue_cr,sum(f.net_income) net_income_cr,sum(f.ocf) ocf_cr,sum(f.fcf) fcf_cr,100.0*sum(CASE WHEN f.revenue>0 AND f.net_income IS NOT NULL THEN f.net_income END)/NULLIF(sum(CASE WHEN f.revenue>0 AND f.net_income IS NOT NULL THEN f.revenue END),0) net_margin_pct,'CURRENT_MEMBERSHIP' membership_basis FROM members CROSS JOIN periods p LEFT JOIN vw_company_financial_base f ON f.company_id=members.company_id AND f.period_end=p.period_end AND f.frequency='ANNUAL' GROUP BY group_name,p.period_end''')
def evidence(db):
 out=Path('reports/intelligence');out.mkdir(parents=True,exist_ok=True)
 report={'release':'8.0-candidate','allocated':db.storage(),'occupied':db.occupied(),'reusable':db.storage()-db.occupied(),'views':{}}
 for view in ['vw_company_financial_coverage','vw_index_acquisition_progress','vw_company_financial_growth','vw_company_profitability','vw_company_financial_strength','vw_sector_financial_summary','vw_factor_financial_summary','vw_industry_financial_summary','vw_index_overlap']:
  report['views'][view]=db.run('SELECT count(*) FROM '+view)[0][0]
 (out/'summary.json').write_text(json.dumps(report,indent=2))
if __name__=='__main__':
 if not os.getenv('TURSO_DATABASE_URL') or not os.getenv('TURSO_AUTH_TOKEN'):raise RuntimeError('Remote configuration required')
 db=DB();install(db);evidence(db)
