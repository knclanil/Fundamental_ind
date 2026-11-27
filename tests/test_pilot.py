from nse_extractor import parse,period,scope,MAP
from quality_checks import checks
from database import DB
import pytest
XML=b'''<xbrl xmlns="http://www.xbrl.org/2003/instance" xmlns:c="urn:in-capmkt" xmlns:i="http://www.xbrl.org/2003/iso4217"><context id="a"><entity><identifier scheme="isin">INE002A01018</identifier></entity><period><instant>2026-03-31</instant></period></context><unit id="u"><measure>i:INR</measure></unit><c:Assets contextRef="a" unitRef="u" decimals="-7">100000000</c:Assets><c:EquityAndLiabilities contextRef="a" unitRef="u" decimals="-7">100000000</c:EquityAndLiabilities></xbrl>'''
def test_parse():
 f,q=parse(XML,'CONSOLIDATED');assert len(f)==2 and not q;assert f[0]['value']=='100000000';assert checks(f)[0]['status']=='PASS'
def test_scope():assert scope('Non-Consolidated')=='STANDALONE'
def test_period():assert period('2025-04-01','2026-03-31')=='ANNUAL'
def test_mapping():assert MAP['EquityAndLiabilities'][1]!='LIABILITIES';assert MAP['InterestEarned'][1]!='REVENUE'
def test_html():
 with pytest.raises(ValueError):parse(b'<html/>','STANDALONE')
def test_schema(monkeypatch,tmp_path):
 monkeypatch.delenv('TURSO_DATABASE_URL',raising=False);monkeypatch.setenv('LOCAL_DB',str(tmp_path/'db'));d=DB();d.run('CREATE TABLE legacy_test(x)');d.init();d.init();assert d.run("SELECT name FROM sqlite_master WHERE name='legacy_test'");assert d.run('SELECT * FROM vw_fund_pnl_statement')==[]
def test_load(monkeypatch,tmp_path):
 from main import load
 monkeypatch.chdir(tmp_path);monkeypatch.delenv('TURSO_DATABASE_URL',raising=False);d=DB();d.init();args=(d,'RELIANCE','financial-results',{'consolidated':'Consolidated'},'https://nsearchives.nseindia.com/a.xml',XML);load(*args);load(*args);assert d.run('SELECT COUNT(*) FROM ft_financials')[0][0]==2;assert d.run('SELECT assets FROM vw_fund_balance_sheet')[0][0]==10;assert d.run('SELECT COUNT(*) FROM vw_fund_verified_financials')[0][0]==0

def test_blocked_audit(monkeypatch,tmp_path):
 import main,sys,json
 monkeypatch.chdir(tmp_path)
 def denied(self,*args,**kwargs):
  self.blocked=True;self.diagnostics.append({'status':403});raise RuntimeError('Access restricted 403')
 monkeypatch.setattr(main.NSE,'get',denied)
 monkeypatch.setattr(sys,'argv',['main.py','--source-audit','--symbol','RELIANCE'])
 assert main.main()==2
 assert json.loads((tmp_path/'reports/ingestion_summary.json').read_text())['status']=='blocked'
 assert len(json.loads((tmp_path/'reports/source_audit.json').read_text()))==1

def test_direct_discovery(monkeypatch):
 from nse_extractor import NSE
 c=NSE();calls=[]
 def get(url,params):
  calls.append((url,dict(params)));return b'[]'
 monkeypatch.setattr(c,'get',get)
 assert len(list(c.discover('RELIANCE',2025,'financial-results')))==3
 assert all(url=='https://www.nseindia.in/api/financial-results' for url,p in calls)
