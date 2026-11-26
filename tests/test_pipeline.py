from pipeline import DB,parse_xml,ingest
import pytest
XML=b'''<xbrl xmlns="http://www.xbrl.org/2003/instance" xmlns:a="urn:test"><context id="c"><period><instant>2025-03-31</instant></period></context><unit id="u"><measure>INR</measure></unit><a:Revenue contextRef="c" unitRef="u" decimals="0">123</a:Revenue></xbrl>'''
def test_parse():
    assert parse_xml(XML)[0][3]=='123'
    with pytest.raises(ValueError): parse_xml(b'<html/>')
def test_idempotent(tmp_path):
    db=DB(str(tmp_path/'test.db')); db.init(); db.init()
    class Source:
        def rows(self,*a): return [{'symbol':'TEST','xbrl':'https://www.nseindia.com/a.xml'}]
        def get(self,*a): return XML
    for _ in range(2): ingest(db,Source(),'ISIN','TEST','annual',2025)
    assert db.run('SELECT COUNT(*) FROM facts')==[(1,)]
    assert db.run('SELECT COUNT(*) FROM filings')==[(1,)]

def test_circuit_breaker(tmp_path):
    from pipeline import work,SourceError
    db=DB(str(tmp_path/'test.db')); db.init()
    db.run("INSERT INTO companies(isin,symbol) VALUES('X','TEST')")
    for d in ('annual','quarterly'): db.run('INSERT INTO tasks(isin,dataset,year) VALUES(?,?,?)','X',d,2026)
    class Blocked:
        def rows(self,*a): raise SourceError('HTTP 403',True)
    with pytest.raises(RuntimeError): work(db,Blocked(),60)
    assert db.run("SELECT COUNT(*) FROM tasks WHERE status='retry' AND error LIKE '%403%'")==[(1,)]
    assert db.run("SELECT COUNT(*) FROM tasks WHERE status='pending'")==[(1,)]

def test_redirect_and_status(monkeypatch):
    from pipeline import Source,SourceError
    import pipeline
    monkeypatch.setattr(pipeline.time,'sleep',lambda _:None)
    class Response:
        def __init__(self,status,url,headers=None): self.status_code=status; self.url=url; self.headers=headers or {}; self.content=b'[]'
    source=Source(); responses=iter([Response(302,'https://www.nseindia.com/a',{'Location':'/b'}),Response(200,'https://www.nseindia.com/b')])
    monkeypatch.setattr(source.s,'get',lambda *a,**k:next(responses))
    assert source.get('https://www.nseindia.com/a')==b'[]'
    monkeypatch.setattr(source.s,'get',lambda *a,**k:Response(403,a[0]))
    with pytest.raises(SourceError,match='HTTP 403'): source.get('https://www.nseindia.com/a')
