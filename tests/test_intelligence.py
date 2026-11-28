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
