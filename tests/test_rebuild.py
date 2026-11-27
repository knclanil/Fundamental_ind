import gzip,hashlib
from database import DB
from fleet import seed,publish,install_wide
from tickers import ticker_for

def test_rebuild(tmp_path):
 d=DB(str(tmp_path/'test.db'));d.init();d.init();seed(d,{'HEGRAPHITE':('INE000000001','Test','Graphite')});cid=d.run('SELECT id FROM dm_company')[0][0]
 row=dict(symbol='HEGRAPHITE',statement='PNL',metric='Total Revenue',frequency='ANNUAL',period_end='2026-03-31',unit='INR',value_raw='123456789.123');cov=[{'symbol':'HEGRAPHITE','currency':'INR'}]
 for v in ['123456789.123','123456789.123','200','123456789.123']:
  publish(d,cid,'HEGRAPHITE',[dict(row,value_raw=v)],cov,{'profile':{'a':'x'*1000}},[])
 assert d.run('SELECT count(*) FROM ft_financials')[0][0]==2
 assert d.run('SELECT count(*) FROM ft_source_reference')[0][0]==4
 assert d.run('SELECT native_value FROM vw_fund_observations')[0][0]=='123456789.123'
 b,h=d.run('SELECT payload,sha FROM dm_payload')[0];assert hashlib.sha256(gzip.decompress(b)).digest()==h
 assert d.run('SELECT count(*) FROM dm_payload')[0][0]==1
 install_wide(d);assert d.run('SELECT total_revenue FROM vw_fund_pnl_wide')[0][0]>12
 assert d.run('PRAGMA integrity_check')==[('ok',)];assert not d.run('PRAGMA foreign_key_check')
 assert ticker_for('BAGMANE')=='BAGMANE.BO';assert ticker_for('HEGRAPHITE')=='GRAPHITE.BO'
def test_legacy_refusal(tmp_path):
 import pytest
 d=DB(str(tmp_path/'legacy.db'));d.run('CREATE TABLE legacy(x)')
 with pytest.raises(RuntimeError):d.init()
