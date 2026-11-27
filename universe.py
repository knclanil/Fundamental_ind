"""Official constituent CSV; index names do not guarantee exact security counts."""
import csv,io,re,requests
from pathlib import Path
URL='https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv'
def parse(raw):
    reader=csv.DictReader(io.StringIO(raw.lstrip('\ufeff')))
    reader.fieldnames=[h.strip().lstrip('\ufeff') for h in (reader.fieldnames or [])]
    required={'Symbol','ISIN Code','Industry','Company Name'}
    if not required.issubset(reader.fieldnames or []):raise ValueError(f'Unexpected constituent CSV headers: {str(reader.fieldnames)[:250]}')
    result={}
    for r in reader:
        s=r['Symbol'].strip(); isin=r['ISIN Code'].strip()
        if not re.fullmatch(r'[A-Z0-9&._-]+',s) or not re.fullmatch(r'IN[A-Z0-9]{10}',isin) or s in result:raise ValueError('Invalid constituent')
        result[s]=(isin,r['Industry'].strip(),r['Company Name'].strip())
    if not 500<=len(result)<=510 or len({v[0] for v in result.values()})!=len(result):
        raise ValueError(f'Unexpected constituent inventory: {len(result)} symbols, {len({v[0] for v in result.values()})} unique ISINs; expected 500–510')
    return result

def fetch():
    out=Path('reports/fleet');out.mkdir(parents=True,exist_ok=True)
    errors=[]
    for attempt in range(3):
        try:
            r=requests.get(URL,headers={'User-Agent':'Mozilla/5.0','Accept':'text/csv,*/*'},timeout=30);r.raise_for_status()
            text=r.content.decode('utf-8-sig')
            members=parse(text)
            (out/'constituents.csv').write_text(text,encoding='utf-8')
            print(f'Official constituent count: {len(members)}',flush=True);return members
        except (requests.RequestException,ValueError,UnicodeError) as e:
            errors.append(f'{type(e).__name__}: {e}')
            if attempt<2:
                import time
                time.sleep(2**attempt)
    raise RuntimeError('Nifty 500 refresh failed: '+'; '.join(errors))

def refresh(db,out):
    import json
    try:
        members=fetch()
    except RuntimeError as e:
        rows=db.run("SELECT c.symbol,c.isin,c.industry,c.name FROM dm_company c JOIN dm_ticker t ON t.company_id=c.id WHERE c.active=1 AND EXISTS (SELECT 1 FROM misc_queue q WHERE q.company_id=c.id AND q.dataset_group='CORE') AND EXISTS (SELECT 1 FROM misc_queue q WHERE q.company_id=c.id AND q.dataset_group='EXTENDED')")
        cached={s:(i,d,n) for s,i,d,n in rows}
        if not 500<=len(cached)<=510 or len({v[0] for v in cached.values()})!=len(cached):
            (out/'universe_status.json').write_text(json.dumps({'status':'BLOCKED','error':str(e),'cached_count':len(cached)}))
            raise
        (out/'universe_status.json').write_text(json.dumps({'status':'EXISTING_DATABASE_UNIVERSE','error':str(e),'count':len(cached)}))
        print('WARNING: '+str(e)+'; continuing existing queue without reseeding',flush=True)
        return None
    (out/'universe_status.json').write_text(json.dumps({'status':'LIVE_VALIDATED','count':len(members)}))
    return members
