from yahoo_deep import audit,inventory

def test_missing_is_not_zero():
    r=dict(symbol='RELIANCE',statement='PNL',frequency='ANNUAL',period_end='2022-03-31',metric='Total Revenue',unit='INR',value_raw='0')
    a=audit([r]); assert next(x for x in a if all(x[k]==r[k] for k in ('symbol','statement','frequency','period_end','metric')))['status']=='AVAILABLE'
    assert any(x['status']=='MISSING' for x in a)

def test_distinct_revenue_codes():
    a=inventory([]); assert len({x['canonical_code'] for x in a if x['source_label'] in ('Total Revenue','Operating Revenue')})==2
