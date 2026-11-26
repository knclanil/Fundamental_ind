import datetime as dt
from pilot import bounds,tolerance,validate
from reliable import init,DB,parse
from test_pipeline import XML
from decimal import Decimal

def test_window():
    assert bounds(dt.date(2026,11,26))[0]==dt.date(2021,4,1)
    assert bounds(dt.date(2026,2,1))[0]==dt.date(2020,4,1)

def test_precision():
    assert tolerance('-2')==Decimal(50)
    assert tolerance(None) is None

def test_entity():
    assert parse(XML.replace(b'>123<',b'>&mdash;<'))[0][6]=='—'

def test_gate(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path); db=DB(str(tmp_path/'db')); init(db)
    result=validate(db); assert not result['accepted']; assert result['verified_facts']==0
