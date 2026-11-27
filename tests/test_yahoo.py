import sqlite3
from yahoo_pilot import number,unit,store,checks,DB

def test_units():
    assert number(float('nan')) is None
    assert number('100.00')=='100'
    assert unit('Basic EPS','INR')=='INR/share'
    assert unit('Unknown metric','INR')=='UNKNOWN'

def test_store():
    db=DB.__new__(DB);db.local=sqlite3.connect(':memory:')
    db.run('CREATE TABLE legacy(x)')
    r=dict(symbol='INFY',statement='PNL',frequency='ANNUAL',period_end='2026-03-31',metric='Total Revenue',unit='INR',value_raw='10000000')
    store(db,[r],'a','hash',{});store(db,[r],'b','hash',{})
    assert db.run('SELECT count(*) FROM ft_yahoo_financials')==[(1,)]
    assert db.run('SELECT value_crores FROM vw_fund_yahoo_pnl')==[(1.0,)]
    assert db.run('SELECT count(*) FROM legacy')==[(0,)]
    assert checks([r])[0]['status']=='NOT_EVALUATED'
