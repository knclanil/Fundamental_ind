"""Offline, non-destructive migration of a consistent SQLite export."""
import argparse, sqlite3, gzip, base64, hashlib, json
from pathlib import Path
PREFIX='gzip64:v1:'
def pack(raw):
    encoded=PREFIX+base64.b64encode(gzip.compress(raw.encode(),mtime=0)).decode()
    return encoded if len(encoded)<len(raw) else raw
def unpack(raw):
    return gzip.decompress(base64.b64decode(raw[len(PREFIX):])).decode() if raw.startswith(PREFIX) else raw

def migrate(source,target):
    source,target=Path(source),Path(target)
    if target.exists():raise FileExistsError(target)
    src=sqlite3.connect(source.resolve().as_uri()+'?mode=ro',uri=True)
    dst=sqlite3.connect(target)
    try:
        src.backup(dst)
        if dst.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise ValueError('Input integrity failure')
        before=dst.execute('PRAGMA page_count').fetchone()[0]*dst.execute('PRAGMA page_size').fetchone()[0]
        views=dst.execute("SELECT name,sql FROM sqlite_schema WHERE type='view'").fetchall()
        with dst:
            for name,_ in views:dst.execute('DROP VIEW "'+name.replace('"','""')+'"')
            dst.execute('CREATE TABLE dm_compact_snapshot(id INTEGER PRIMARY KEY,sha TEXT NOT NULL,retrieved TEXT NOT NULL,UNIQUE(sha,retrieved))')
            for table in ('ft_yahoo_source_reference','ft_yahoo_classification'):
                dst.execute(f'INSERT OR IGNORE INTO dm_compact_snapshot(sha,retrieved) SELECT DISTINCT snapshot_sha,retrieved FROM {table}')
                extra=table.endswith('classification'); cols=',classified_unit TEXT,unit_kind TEXT' if extra else ''; select=',t.classified_unit,t.unit_kind' if extra else ''; fields=',classified_unit,unit_kind' if extra else ''
                dst.execute(f'CREATE TABLE {table}_compact(fact_id INTEGER NOT NULL,snapshot_id INTEGER NOT NULL{cols},PRIMARY KEY(fact_id,snapshot_id)) WITHOUT ROWID')
                dst.execute(f'INSERT INTO {table}_compact SELECT t.fact_id,s.id{select} FROM {table} t JOIN dm_compact_snapshot s ON s.sha=t.snapshot_sha AND s.retrieved=t.retrieved')
                dst.execute(f'ALTER TABLE {table} RENAME TO {table}_old')
                dst.execute(f'CREATE VIEW {table} AS SELECT fact_id,sha AS snapshot_sha,retrieved{fields} FROM {table}_compact JOIN dm_compact_snapshot ON id=snapshot_id')
                for a,b in ((table,table+'_old'),(table+'_old',table)):
                    if dst.execute(f'SELECT * FROM {a} EXCEPT SELECT * FROM {b} LIMIT 1').fetchone():raise ValueError('Migration mismatch '+table)
                dst.execute(f'DROP TABLE {table}_old')
                vals=',NEW.classified_unit,NEW.unit_kind' if extra else ''
                dst.execute(f'''CREATE TRIGGER insert_{table} INSTEAD OF INSERT ON {table} BEGIN INSERT OR IGNORE INTO dm_compact_snapshot(sha,retrieved) VALUES(NEW.snapshot_sha,NEW.retrieved); INSERT OR IGNORE INTO {table}_compact SELECT NEW.fact_id,id{vals} FROM dm_compact_snapshot WHERE sha=NEW.snapshot_sha AND retrieved=NEW.retrieved; END''')
            for rowid,raw,sha in dst.execute('SELECT rowid,payload,sha FROM ft_yahoo_extended').fetchall():
                if hashlib.sha256(raw.encode()).hexdigest()!=sha:raise ValueError('Payload hash mismatch')
                compressed=pack(raw)
                if unpack(compressed)!=raw:raise ValueError('Compression mismatch')
                dst.execute('UPDATE ft_yahoo_extended SET payload=? WHERE rowid=?',(compressed,rowid))
            for _,sql in views:dst.execute(sql)
        dst.execute('VACUUM')
        if dst.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise ValueError('Output integrity failure')
        return {'before_allocated_bytes':before,'after_allocated_bytes':target.stat().st_size,'integrity':'ok'}
    except BaseException:
        dst.close();target.unlink(missing_ok=True);raise
    finally:src.close();dst.close()
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('source');p.add_argument('target');a=p.parse_args();print(json.dumps(migrate(a.source,a.target),indent=2))
