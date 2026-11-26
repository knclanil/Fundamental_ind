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

def test_worker_strict_sql(tmp_path, monkeypatch):
    import reliable
    import sqlite3
    db=DB(str(tmp_path/'worker.db'))
    init(db)
    # Reject double-quoted SQL tokens to reproduce Turso strict literal handling.
    original=db.run
    def strict(sql,*args):
        assert '"' not in sql, sql
        return original(sql,*args)
    monkeypatch.setattr(db,'run',strict)
    year=reliable.dt.date.today().year
    db.run("INSERT INTO companies(isin,symbol) VALUES('X','TEST')")
    db.run("INSERT INTO v2_listings VALUES('X','NSE','TEST')")
    db.run("INSERT INTO pilot_members VALUES('TEST')")
    for y,state in [(year,'pending'),(year-2,'pending'),(year-3,'not_applicable')]:
        db.run("INSERT INTO v2_jobs(isin,source,dataset,year,state) VALUES('X','NSE','legacy',?,?)",y,state)
    seen=[]
    def fake(db,source,isin,exchange,code,dataset,y):
        seen.append(y)
        return 'empty','fixture'
    monkeypatch.setattr(reliable,'ingest',fake)
    import pytest
    with pytest.raises(RuntimeError,match='No validated financial facts'):
        reliable.work(db,None,60)
    assert seen==[year,year-2]
    assert db.run("SELECT state FROM v2_jobs WHERE year=?",year-3)==[('not_applicable',)]
    assert db.run("SELECT state FROM v2_jobs WHERE year=?",year)==[('empty',)]

def test_audit_separates_ownership(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path); db=DB(str(tmp_path/'audit.db')); init(db)
    db.run("INSERT INTO companies(isin,symbol) VALUES('X','TEST')")
    db.run("INSERT INTO v2_filings(id,isin,source,dataset,status) VALUES('F','X','NSE','shareholding','parsed')")
    db.run("INSERT INTO v2_facts(id,filing,concept,end,status) VALUES('A','F','Shares','2025-03-31','unverified')")
    result=report(db); assert result['audit']['financial_companies']==0
    assert result['audit']['dataset_facts'][0][1]=='shareholding'

def test_budget_guard(tmp_path,monkeypatch):
    import reliable,pytest
    db=DB(str(tmp_path/'budget.db')); init(db)
    monkeypatch.setenv('DATABASE_BYTE_BUDGET','1')
    with pytest.raises(RuntimeError,match='storage budget reached'): reliable.work(db,None,60)
