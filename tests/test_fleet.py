import pytest,sqlite3
from universe import parse
from database import DB

def test_universe():
    raw='Company Name,Industry,Symbol,ISIN Code\n'+'\n'.join(f'Company {i},Bank,S{i},INE{i:09d}' for i in range(500))
    assert len(parse(raw))==500
    with pytest.raises(ValueError):parse(raw.rsplit('\n',1)[0])

def test_reusable_pages():
    db=DB.__new__(DB);db.local=sqlite3.connect(':memory:');db.run('CREATE TABLE t(x)');db.run('INSERT INTO t VALUES(zeroblob(100000))');db.run('DROP TABLE t');assert db.occupied()<db.storage()
