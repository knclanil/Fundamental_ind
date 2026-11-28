import base64
import pytest
from database import DB,decode_blob

@pytest.mark.parametrize('raw',[b'',b'a',b'ab',b'abc',bytes(range(256))])
def test_blob_padding(raw):
 encoded=base64.b64encode(raw).decode()
 assert decode_blob(encoded)==raw
 assert decode_blob(encoded.rstrip('='))==raw

def test_remote_blob(monkeypatch):
 monkeypatch.setenv('TURSO_DATABASE_URL','libsql://example.invalid')
 monkeypatch.setenv('TURSO_AUTH_TOKEN','test')
 db=DB()
 class Response:
  def raise_for_status(self):pass
  def json(self):return {'results':[{'type':'ok','response':{'result':{'rows':[[{'type':'blob','base64':'YQ'}]]}}}]}
 monkeypatch.setattr(db.session,'post',lambda *a,**k:Response())
 assert db.run('SELECT payload')==[[b'a']]

def test_invalid_blob():
 with pytest.raises(ValueError):decode_blob('!bad')
