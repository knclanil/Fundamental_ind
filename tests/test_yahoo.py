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

def test_partial_and_batch(monkeypatch):
    from yahoo_pilot import WATCH,publishable
    rows=[dict(symbol=s,statement=t,frequency='ANNUAL',period_end='2026-03-31',metric='Example',unit='USD',value_raw='10') for s in WATCH for t in ('PNL','BS','CF')]
    assert publishable(rows) and not publishable(rows[:-1])
    db=DB.__new__(DB);db.local=sqlite3.connect(':memory:')
    monkeypatch.setenv('YAHOO_IGNORE_STORAGE_BUDGET','true')
    summary={};store(db,rows,'a','hash',summary,True)
    assert summary['published_unique_facts']==15
    store(db,rows,'a','hash',summary,True)
    assert db.run('SELECT count(*) FROM ft_yahoo_financials')==[(15,)]
