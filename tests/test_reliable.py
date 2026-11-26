from reliable import parse,init,DB,report
from test_pipeline import XML

def test_context():
    f=parse(XML)[0]; assert f[3]=='2025-03-31'; assert f[6]=='123'; assert f[-1]=='unverified'

def test_additive(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path); db=DB(str(tmp_path/'db')); db.init(); db.run("INSERT INTO companies(isin,symbol) VALUES('X','TEST')"); init(db); init(db); assert report(db)['companies']==1

def test_bad_period():
    assert parse(XML.replace(b'2025-03-31',b'bad'))[0][-1]=='quarantined'

def test_integrated_pagination():
    import json
    from reliable import discover
    class Fake:
        def get(self,url,params):
            assert url.endswith('/integrated-filing-results')
            assert params['type']=='Integrated Filing- Financials'
            return json.dumps({'data':[{'symbol':'TEST','id':params['page']}], 'totalCount':2}).encode()
    assert len(discover(Fake(),'NSE','TEST','integrated',2025))==2

def test_legacy_periods():
    import json
    from reliable import discover
    class Fake:
        def get(self,url,params):
            return json.dumps([{'period':params['period']}]).encode()
    assert [r['period'] for r in discover(Fake(),'NSE','TEST','legacy',2024)]==['Quarterly','Annual','Half-Yearly']
