from database import DB
from intelligence import install
from membership import publish
from test_membership import csv

def test_install_and_overlap(tmp_path):
 d=DB(str(tmp_path/'db'));install(d);install(d)
 for iid,sid in d.run('SELECT index_id,id FROM dm_data_source LIMIT 2'):
  publish(d,iid,sid,csv(range(5)))
 assert d.run('SELECT common_count,jaccard FROM vw_index_overlap')==[(5,1.0)]
 for v in ['vw_company_financial_growth','vw_company_profitability','vw_company_financial_strength','vw_company_cashflow_quality','vw_factor_financial_summary','vw_sector_financial_summary','vw_industry_financial_summary','vw_index_financial_summary']:
  d.run('SELECT * FROM '+v)
 assert not d.run('PRAGMA foreign_key_check')

def test_financial_upgrade(tmp_path):
 from fleet import seed,publish,extended_plan,record_modules,extended_status
 d=DB(str(tmp_path/'financial.db'));d.init();seed(d,{'COALINDIA':('INE000000001','Mining','Coal India')});cid=d.run('SELECT id FROM dm_company')[0][0]
 d.run('CREATE VIEW vw_company_financial_base AS SELECT 1 obsolete')
 rows=[]
 for stmt,metric,value in [('PNL','Total Revenue','1664173000000'),('PNL','Net Income','310942900000'),('PNL','Basic EPS','50.46'),('BS','Total Assets','2856602700000'),('CF','Operating Cash Flow','432146000000'),('CF','Investing Cash Flow','-339545800000'),('CF','Financing Cash Flow','-118006700000')]:
  rows.append(dict(symbol='COALINDIA',statement=stmt,metric=metric,frequency='ANNUAL',period_end='2026-03-31',unit='INR',value_raw=value))
 publish(d,cid,'COALINDIA',rows,[{'symbol':'COALINDIA','currency':'INR'}],{},[])
 install(d);install(d)
 assert d.run('SELECT revenue,basic_eps,investing_cash_flow,financing_cash_flow FROM vw_company_financial_base')==[(166417.3,50.46,-33954.58,-11800.67)]
 assert d.run('SELECT count(*) FROM vw_missing_financial_periods')==[(4,)]
 assert d.run('SELECT count(*) FROM ft_financials')==[(7,)]
 extended_plan(d,cid);record_modules(d,cid,[{'module':m,'status':'AVAILABLE'} for m in ('prices','shares')]);assert extended_status(d,cid)=='PUBLISHED_UNVERIFIED'
 assert not d.run('PRAGMA foreign_key_check')
