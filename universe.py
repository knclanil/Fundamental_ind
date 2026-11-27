"""Official constituent CSV; index names do not guarantee exact security counts."""
import csv,io,re,requests
from pathlib import Path
URL='https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv'
def parse(raw):
    reader=csv.DictReader(io.StringIO(raw.lstrip('\ufeff')))
    required={'Symbol','ISIN Code','Industry','Company Name'}
    if not required.issubset(reader.fieldnames or []):raise ValueError('Unexpected constituent CSV headers')
    result={}
    for r in reader:
        s=r['Symbol'].strip(); isin=r['ISIN Code'].strip()
        if not re.fullmatch(r'[A-Z0-9&._-]+',s) or not re.fullmatch(r'IN[A-Z0-9]{10}',isin) or s in result:raise ValueError('Invalid constituent')
        result[s]=(isin,r['Industry'].strip(),r['Company Name'].strip())
    if not 500<=len(result)<=510 or len({v[0] for v in result.values()})!=len(result):
        raise ValueError(f'Unexpected constituent inventory: {len(result)} symbols, {len({v[0] for v in result.values()})} unique ISINs; expected 500–510')
    return result

def fetch():
    r=requests.get(URL,headers={'User-Agent':'Mozilla/5.0','Accept':'text/csv,*/*'},timeout=60);r.raise_for_status()
    out=Path('reports/fleet');out.mkdir(parents=True,exist_ok=True)
    (out/'constituents.csv').write_text(r.text,encoding='utf-8')
    members=parse(r.text);print(f'Official constituent count: {len(members)}',flush=True);return members
