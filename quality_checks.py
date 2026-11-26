from decimal import Decimal
from collections import defaultdict

def checks(facts):
    groups=defaultdict(dict)
    for f in facts:
        if f['status']=='CANDIDATE' and f['dimensions']=='[]' and f['unit']=='INR':groups[(f['scope'],f['start'],f['end'])].setdefault(f['code'],[]).append(f)
    out=[]
    for key,g in groups.items():
        for name,terms,fallback in [('BS',{'ASSETS':1,'EQUITY_LIABILITIES':-1},10_000_000),('BS_COMPONENTS',{'ASSETS':1,'EQUITY':-1,'LIABILITIES':-1},10_000_000),('CF',{'CFO':1,'CFI':1,'CFF':1,'NET_CHANGE_BEFORE_FX':-1},20_000_000)]:
            if not all(t in g for t in terms):continue
            if any(len({f['value'] for f in g[t]})!=1 for t in terms):out.append(dict(group=key,check=name,status='DISPUTED'));continue
            rows=[g[t][0] for t in terms]; delta=sum(Decimal(g[t][0]['value'])*s for t,s in terms.items()); tol=Decimal(0)
            for f in rows:
                d=f['decimals']
                if d=='INF':continue
                try:tol+=Decimal(10)**(-int(d))/2
                except (TypeError,ValueError):tol=Decimal(fallback);break
            out.append(dict(group=key,check=name,status='PASS' if abs(delta)<=tol else 'WARNING',delta=str(delta),tolerance=str(tol)))
    return out
