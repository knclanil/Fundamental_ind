import pytest,json
import universe
from database import DB
from fleet import seed

def test_headers():
 text=' Company Name , Industry , Symbol , ISIN Code \n'+''.join(f'Name,Industry,S{i},INE{i:09d}\n' for i in range(501))
 assert len(universe.parse(text))==501
 with pytest.raises(ValueError):universe.parse('<html>blocked</html>')
 with pytest.raises(ValueError):universe.parse(text.splitlines()[0]+'\n'+text.splitlines()[1])

def test_fallback(tmp_path,monkeypatch):
 db=DB(str(tmp_path/'db'));db.init()
 def fail():raise RuntimeError('source unavailable')
 monkeypatch.setattr(universe,'fetch',fail)
 with pytest.raises(RuntimeError):universe.refresh(db,tmp_path)
 seed(db,{f'S{i}':(f'INE{i:09d}','Industry','Name') for i in range(501)})
 before=db.run('SELECT * FROM misc_queue')
 assert universe.refresh(db,tmp_path) is None
 assert db.run('SELECT * FROM misc_queue')==before
 assert json.loads((tmp_path/'universe_status.json').read_text())['status']=='EXISTING_DATABASE_UNIVERSE'
