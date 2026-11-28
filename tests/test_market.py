import datetime as dt
import pytest
from database import DB
from market import install,parse,publish

def test_market(tmp_path):
 db=DB(str(tmp_path/'test.sqlite'));install(db);install(db)
 db.run("INSERT INTO dm_company(id,isin,symbol) VALUES(1,'TEST','TEST')")
 dates=[(dt.date(2025,1,1)+dt.timedelta(days=i)).isoformat() for i in range(22)]
 rows=parse({'columns':['Close','Volume'],'index':dates,'data':[[100+i,100 if i<21 else 200] for i in range(22)]})
 assert publish(db,'COMPANY',1,rows,'TEST')==22
 assert publish(db,'COMPANY',1,rows,'TEST')==0
 r=db.run('SELECT relative_volume,daily_return_pct,return_21d_pct FROM vw_company_market_performance ORDER BY trading_date DESC LIMIT 1')[0]
 assert r==pytest.approx((2,100*(121/120-1),21))
 assert db.run('PRAGMA foreign_key_check')==[]
 assert db.run('SELECT sessions,volume_sessions FROM vw_market_data_coverage WHERE entity_id=1')==[(22,22)]

def test_invalid():
 with pytest.raises(ValueError):parse({'columns':['Close','Volume'],'index':['2025-01-01'],'data':[[10,-1]]})
 assert parse({'columns':['Close'],'index':['2025-01-01T18:30:00.000Z'],'data':[[10]]})[0][0]=='2025-01-02'

def test_recovery_and_diagnostics(tmp_path):
 from market import run
 db=DB(str(tmp_path/'recovery.sqlite'));install(db)
 for i in range(1,4):db.run('INSERT INTO dm_company(id,isin,symbol) VALUES(?,?,?)',(i,str(i),'TEST'+str(i)))
 db.run("INSERT INTO misc_market_data_queue VALUES('COMPANY',3,'SOURCE_UNAVAILABLE','2020-01-01T00:00:00+00:00',NULL,NULL)")
 class Fake:
  def Ticker(self,ticker):
   self.ticker=ticker;return self
  def history(self,**kwargs):
   assert 'raise_errors' not in kwargs
   if self.ticker=='TEST3.NS':raise RuntimeError('specific provider failure')
   return {'columns':['Close','Volume'],'index':[dt.date.today().isoformat()],'data':[[10,100]]}
 r=run(db,limit=2,yf=Fake())
 assert [x['id'] for x in r['attempts']]==[3,1]
 assert r['errors'][0]['error']=='specific provider failure'
 assert r['errors'][0]['stage']=='DOWNLOAD'
 assert r['changed']==1

def test_database_failure(tmp_path,monkeypatch):
 import market
 db=DB(str(tmp_path/'db.sqlite'));install(db)
 db.run("INSERT INTO dm_company(id,isin,symbol) VALUES(1,'TEST','TEST')")
 class Fake:
  def Ticker(self,ticker):return self
  def history(self,**kwargs):return {'columns':['Close'],'index':[dt.date.today().isoformat()],'data':[[10]]}
 def fail(*args):raise RuntimeError('database rejected write')
 monkeypatch.setattr(market,'publish',fail)
 r=market.run(db,yf=Fake())
 assert r['outcome']=='DATABASE_FAILURE_STOP'
 assert r['errors'][0]['stage']=='DATABASE_PUBLICATION'
 assert db.run('SELECT status FROM misc_market_data_queue')==[('DATABASE_FAILED',)]

def test_transient_retry(monkeypatch):
 import market
 class Fake:
  calls=0
  def Ticker(self,ticker):return self
  def history(self,**kwargs):
   self.calls+=1
   if self.calls<3:raise RuntimeError('429 rate limit')
   return 'ok'
 monkeypatch.setattr(market.time,'sleep',lambda x:None)
 f=Fake();assert market.download(f,'TEST','a','b',float('inf'))=='ok';assert f.calls==3
