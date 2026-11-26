import os,sqlite3,requests
class DB:
    def __init__(self):
        self.url=os.getenv('TURSO_DATABASE_URL'); self.token=os.getenv('TURSO_AUTH_TOKEN'); self.local=None
        if self.url:
            if not self.token:raise ValueError('Missing Turso token')
            self.url=self.url.replace('libsql://','https://').rstrip('/')+'/v2/pipeline'
        else:self.local=sqlite3.connect(os.getenv('LOCAL_DB','pilot.sqlite')); self.local.execute('PRAGMA foreign_keys=ON')
    def run(self,sql,args=()):
        if self.local is not None:
            c=self.local.execute(sql,args); rows=c.fetchall(); self.local.commit(); return rows
        vals=[{'type':'null'} if x is None else {'type':'integer','value':str(x)} if isinstance(x,int) else {'type':'text','value':str(x)} for x in args]
        r=requests.post(self.url,headers={'Authorization':'Bearer '+self.token},json={'requests':[{'type':'execute','stmt':{'sql':sql,'args':vals}},{'type':'close'}]},timeout=60); r.raise_for_status(); res=r.json()['results'][0]
        if res['type']=='error':raise RuntimeError(res['error']['message'])
        return [[v.get('value') for v in row] for row in res['response']['result']['rows']]
    def init(self):
        from pathlib import Path
        for s in Path(__file__).with_name('schema.sql').read_text().split(';'):
            if s.strip():self.run(s)
    def id(self,table,**fields):
        cols=','.join(fields); marks=','.join('?' for _ in fields); where=' AND '.join(k+'=?' for k in fields)
        self.run(f'INSERT OR IGNORE INTO {table} ({cols}) VALUES ({marks})',tuple(fields.values())); return int(self.run(f'SELECT id FROM {table} WHERE {where}',tuple(fields.values()))[0][0])
    def storage(self):
        return int(self.run('PRAGMA page_count')[0][0])*int(self.run('PRAGMA page_size')[0][0])
    def guard(self):
        if self.storage()>=int(os.getenv('STORAGE_BUDGET_BYTES','400000000'))*.95:raise RuntimeError('Storage guard: existing database near budget')
