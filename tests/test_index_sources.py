import json
from pathlib import Path
from market import INDEX_TICKERS, NEW_INDEX_TICKERS

def test_registry_coverage():
    registry=json.loads((Path(__file__).parents[1]/'india_index_sources.json').read_text())
    assert len(NEW_INDEX_TICKERS)==17
    assert all(r['code'] in INDEX_TICKERS for r in registry if r['category'] in ('SECTOR','FACTOR'))
    assert INDEX_TICKERS['NIFTY_ALPHA_INDEX']=='NIFTYALPHA50.NS'
    assert INDEX_TICKERS['NIFTY_OIL_GAS']=='NIFTY_OIL_AND_GAS.NS'
