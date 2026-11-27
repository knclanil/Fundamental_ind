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

def test_reusable_pages(tmp_path):
 d=DB(str(tmp_path/'pages.db'));d.run('CREATE TABLE t(x)');d.run('INSERT INTO t VALUES(zeroblob(100000))');d.run('DROP TABLE t');assert d.occupied()<d.storage()

def test_remote_types(monkeypatch):
 import base64
 monkeypatch.setenv('TURSO_DATABASE_URL','libsql://example.invalid');monkeypatch.setenv('TURSO_AUTH_TOKEN','test')
 d=DB()
 class Response:
  def raise_for_status(self):pass
  def json(self):return {'results':[{'type':'ok','response':{'result':{'rows':[[{'type':'integer','value':'3'},{'type':'blob','base64':'YWJj'},{'type':'null'}]]}}}]}
 monkeypatch.setattr(d.session,'post',lambda *a,**k:Response())
 assert d.run('SELECT 1')==[[3,b'abc',None]]

def test_extended_isolation_and_preservation(tmp_path,monkeypatch):
 import json
 from fleet import acquire
 monkeypatch.setenv('YAHOO_DELAY','0')
 class Frame:
  ndim=2
  empty=False
  def to_json(self,**kwargs):
   assert kwargs['orient']=='split'
   return '{"index":["2026-01-01","2026-01-01"],"columns":["Close"],"data":[[1],[2]]}'
 class Bad:
  def to_json(self,**kwargs):raise ValueError('serialization failure')
 class Ticker:
  def history(self,**kwargs):return Frame()
  def get_shares_full(self):return Bad()
  dividends={};splits={'x':2};institutional_holders=None;major_holders=None;mutualfund_holders=None;earnings_estimate=None;revenue_estimate=None;recommendations=None
 rows,cov,payload,errors=acquire(lambda _:Ticker(),'HEGRAPHITE','EXTENDED')
 assert set(payload)=={'prices','splits'}
 assert any('serialization failure' in e for e in errors)
 d=DB(str(tmp_path/'extended.db'));d.init();seed(d,{'HEGRAPHITE':('INE000000001','Test','Graphite')});cid=d.run('SELECT id FROM dm_company')[0][0]
 row=dict(symbol='HEGRAPHITE',statement='PNL',metric='Total Revenue',frequency='ANNUAL',period_end='2026-03-31',unit='INR',value_raw='100')
 publish(d,cid,'HEGRAPHITE',[row],[{'symbol':'HEGRAPHITE','currency':'INR'}],{},[])
 publish(d,cid,'HEGRAPHITE',rows,cov,payload,errors)
 assert d.run('SELECT native_value FROM vw_fund_observations')==[('100',)]
 assert d.run('SELECT count(*) FROM ft_extended')==[(2,)]
 for module,b,h in d.run('SELECT module,payload,sha FROM ft_extended JOIN dm_payload ON payload_id=id'):
  assert hashlib.sha256(gzip.decompress(b)).digest()==h
  assert json.loads(gzip.decompress(b))==payload[module]
 assert not d.run('PRAGMA foreign_key_check')
