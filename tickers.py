"""Provider ticker overrides; canonical NSE universe symbols remain unchanged."""
OVERRIDES={'HEGRAPHITE':'GRAPHITE.BO','BAGMANE':'BAGMANE.BO'}
def ticker_for(symbol):
    return OVERRIDES.get(symbol,symbol+'.NS')
