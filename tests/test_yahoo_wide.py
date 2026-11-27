import sqlite3
from database import DB
from yahoo_pilot import store
from yahoo_quality import persist
from yahoo_wide import install

def test_wide_conflict_and_currency():
    db=DB.__new__(DB);db.local=sqlite3.connect(':memory:')
    rows=[dict(symbol='INFY',statement='PNL',frequency='ANNUAL',period_end='2026-03-31',metric=m,unit='USD',value_raw=v,classified_unit='USD',unit_kind='MONEY') for m,v in [('Total Revenue','2000000'),('Net Income','100000'),('Net Income','200000')]]
    store(db,rows,'2026-11-26','abc',{});persist(db,rows,[],[],'2026-11-26','abc');install(db)
    assert db.run('SELECT revenue,net_income,currency,conflicting_metrics FROM vw_fund_yahoo_pnl_annual')==[(2.0,None,'USD',1)]
