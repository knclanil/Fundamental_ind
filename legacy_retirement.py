"""Inventory live objects; deletion blocked until verified ownership and backup exist."""
import json,os
from pathlib import Path
from database import DB

def audit(db):
    return [dict(type=t,name=n,table=tbl,sql=sql,classification='YAHOO_ACTIVE' if 'yahoo' in n.lower() else 'UNKNOWN_OWNERSHIP') for t,n,tbl,sql in db.run("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")]
if __name__=='__main__':
    if not os.getenv('TURSO_DATABASE_URL'):raise SystemExit('Live credentials required')
    db=DB();out=Path('reports/production');out.mkdir(parents=True,exist_ok=True);(out/'legacy_cleanup_report.json').write_text(json.dumps(dict(status='BLOCKED_NO_VERIFIED_BACKUP_OR_OWNERSHIP',objects=audit(db),storage_bytes=db.storage(),deleted=[]),indent=2))
