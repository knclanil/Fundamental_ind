import sqlite3, hashlib
from optimize_db import migrate,unpack
from tickers import ticker_for
from yahoo_pilot import SCHEMA

def test_migration(tmp_path):
    a,b=tmp_path/'a.db',tmp_path/'b.db'
    c=sqlite3.connect(a)
    for sql in SCHEMA:c.execute(sql)
    c.execute('CREATE TABLE ft_yahoo_classification(fact_id INTEGER,snapshot_sha TEXT,retrieved TEXT,classified_unit TEXT,unit_kind TEXT,PRIMARY KEY(fact_id,snapshot_sha,retrieved))')
    c.execute('CREATE TABLE ft_yahoo_extended(symbol TEXT,module TEXT,sha TEXT,payload TEXT,first_seen TEXT,PRIMARY KEY(symbol,module,sha))')
    raw='{"data":"'+'a'*10000+'"}'
    c.execute('INSERT INTO ft_yahoo_extended VALUES(?,?,?,?,?)',('X','profile',hashlib.sha256(raw.encode()).hexdigest(),raw,'date'))
    for i in range(1000):
        c.execute('INSERT INTO ft_yahoo_source_reference VALUES(?,?,?)',(i,'a'*64,'date'))
        c.execute('INSERT INTO ft_yahoo_classification VALUES(?,?,?,?,?)',(i,'a'*64,'date','INR','currency'))
    c.commit();c.close()
    report=migrate(a,b);assert report['after_allocated_bytes']<report['before_allocated_bytes']
    c=sqlite3.connect(b)
    for sql in SCHEMA:c.execute(sql)
    c.execute('INSERT OR IGNORE INTO ft_yahoo_source_reference VALUES(?,?,?)',(1001,'b'*64,'date2'))
    assert c.execute('SELECT count(*) FROM ft_yahoo_source_reference').fetchone()[0]==1001
    assert unpack(c.execute('SELECT payload FROM ft_yahoo_extended').fetchone()[0])==raw
    assert c.execute('SELECT count(*) FROM ft_yahoo_classification').fetchone()[0]==1000
    c.close()

def test_tickers():
    assert ticker_for('HEGRAPHITE')=='GRAPHITE.BO'
    assert ticker_for('BAGMANE')=='BAGMANE.BO'
    assert ticker_for('INFY')=='INFY.NS'
