from yahoo_quality import classify,validate,freshness
from datetime import date

def test_classification():
    rows=[dict(symbol='INFY',statement='BS',metric=m) for m in ('Share Issued','Net PPE','Unseen')]
    assert [r['classified_unit'] for r in classify(rows,[dict(symbol='INFY',currency='USD')])]==['SHARES','USD','UNKNOWN']

def test_freshness():
    rows=[dict(symbol='X',statement='PNL',frequency='QUARTERLY',period_end='2025-06-30')]
    assert freshness(rows,date(2026,11,26))[0]['status']=='STALE'

def test_cash_bridge():
    rows=[dict(symbol='X',statement='CF',frequency='ANNUAL',period_end='2026-03-31',classified_unit='INR',unit_kind='MONEY',metric=m,value_raw=str(v)) for m,v in [('End Cash Position',120),('Beginning Cash Position',100),('Changes In Cash',15),('Effect Of Exchange Rate Changes',5)]]
    assert {r['check']:r['status'] for r in validate(rows)}=={'cash_movement':'NOT_EVALUATED','cash_bridge':'PASS'}
