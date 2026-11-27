"""Official universe, bounded resumable batches; no fabricated constituents."""
import csv,io,re,requests
URL='https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv'
def parse(raw):
    rows=list(csv.DictReader(io.StringIO(raw.lstrip('\ufeff')))); result={}
    for r in rows:
        s=r['Symbol'].strip(); isin=r['ISIN Code'].strip()
        if not re.fullmatch(r'[A-Z0-9&._-]+',s) or not re.fullmatch(r'IN[A-Z0-9]{10}',isin) or s in result:raise ValueError('Invalid constituent')
        result[s]=(isin,r['Industry'].strip(),r['Company Name'].strip())
    if len(result)!=500 or len({v[0] for v in result.values()})!=500:raise ValueError('Expected 500 unique constituents')
    return result

def fetch():
    r=requests.get(URL,headers={'User-Agent':'Mozilla/5.0'},timeout=60);r.raise_for_status();return parse(r.text)
