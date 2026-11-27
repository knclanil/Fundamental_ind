"""Additional candidate analytics; ratios never mix reporting currencies."""
def install_extra(db):
    views={
    'ttm':"SELECT * FROM vw_fund_yahoo_financial_summary WHERE frequency='TTM'",
    'banking_metrics':"SELECT p.symbol,p.frequency,p.period_end,p.currency,p.interest_income,p.interest_expense,p.net_interest_income,p.net_income FROM vw_fund_yahoo_pnl_wide p JOIN dm_yahoo_universe u USING(symbol) WHERE lower(u.industry) LIKE '%bank%' OR p.symbol='HDFCBANK'",
    'profitability':"SELECT a.*, CASE WHEN a.pnl_currency=a.bs_currency AND a.pnl_currency IN ('INR','USD') AND a.total_equity>0 THEN 100.0*a.net_income/a.total_equity END AS return_on_ending_equity_pct, CASE WHEN a.pnl_currency=a.bs_currency AND a.pnl_currency IN ('INR','USD') AND a.total_assets>0 THEN 100.0*a.net_income/a.total_assets END AS return_on_ending_assets_pct FROM vw_fund_yahoo_ratios a WHERE frequency IN ('ANNUAL','TTM')",
    'company_coverage':"SELECT u.*,q.status,q.last_success FROM dm_yahoo_universe u LEFT JOIN misc_yahoo_queue q USING(symbol)",
    'screening':"SELECT a.*,u.industry,u.isin FROM vw_fund_yahoo_ratios a JOIN dm_yahoo_universe u USING(symbol) WHERE u.active=1 AND a.frequency='ANNUAL'",
    'verified_financials':"SELECT * FROM vw_fund_yahoo_financial_summary WHERE 0",
    }
    for name,sql in views.items():
        db.run('CREATE VIEW IF NOT EXISTS vw_fund_yahoo_'+name+' AS '+sql)
