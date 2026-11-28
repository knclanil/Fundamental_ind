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
