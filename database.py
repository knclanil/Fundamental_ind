import os,sqlite3,requests,base64
from pathlib import Path
class DB:
 def __init__(self,path=None):
  self.url=os.getenv('TURSO_DATABASE_URL') if path is None else None; self.token=os.getenv('TURSO_AUTH_TOKEN'); self.local=None; self.session=requests.Session()
  if self.url:
   if not self.token: raise RuntimeError('Missing token')
   self.url=self.url.replace('libsql://','https://').rstrip('/')+'/v2/pipeline'
  else:
   self.local=sqlite3.connect(path or 'fundamental.sqlite'); self.local.execute('PRAGMA foreign_keys=ON')
 def run(self,sql,args=()):
  if self.local is not None:
   rows=self.local.execute(sql,args).fetchall(); self.local.commit(); return rows
  def val(x):
   if x is None:return {'type':'null'}
   if isinstance(x,bytes):return {'type':'blob','base64':base64.b64encode(x).decode()}
   if isinstance(x,float):return {'type':'float','value':x}
   if isinstance(x,int):return {'type':'integer','value':str(x)}
   return {'type':'text','value':str(x)}
  r=self.session.post(self.url,headers={'Authorization':'Bearer '+self.token},json={'requests':[{'type':'execute','stmt':{'sql':sql,'args':[val(x) for x in args]}},{'type':'close'}]},timeout=60);r.raise_for_status();res=r.json()['results'][0]
  if res['type']=='error':raise RuntimeError(res['error']['message'])
  return [[base64.b64decode(v['base64']) if v['type']=='blob' else int(v['value']) if v['type']=='integer' else float(v['value']) if v['type']=='float' else v.get('value') for v in row] for row in res['response']['result']['rows']]
 def init(self):
  names={r[0] for r in self.run("SELECT name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'")}
  if names and 'misc_schema_version' not in names:raise RuntimeError('Nonempty legacy database; refusing initialization')
  if 'misc_schema_version' in names and self.run('SELECT version FROM misc_schema_version') not in ([('6.0',)],[['6.0']],[]):raise RuntimeError('Wrong schema version')
  for s in Path(__file__).with_name('schema.sql').read_text().split(';'):
   if s.strip():self.run(s)
 def storage(self):return int(self.run('PRAGMA page_count')[0][0])*int(self.run('PRAGMA page_size')[0][0])
 def occupied(self):return (int(self.run('PRAGMA page_count')[0][0])-int(self.run('PRAGMA freelist_count')[0][0]))*int(self.run('PRAGMA page_size')[0][0])
