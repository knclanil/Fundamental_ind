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
