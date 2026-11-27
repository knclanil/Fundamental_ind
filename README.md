# Fundamental_ind 2.0 — five-company pilot
Fresh repository, no legacy writes or deletes. This is a locally tested pilot foundation, not a production-certified implementation. Only the five fixed symbols are accepted. No automatic expansion.

## Run
Install `python -m pip install -r requirements.txt`, then `python -m pytest -q`. Without Turso environment variables, commands use local SQLite. Run `python main.py --source-audit --max-jobs 20` to inspect permitted source responses without database writes. Run `python main.py --init-db` to initialize new tables; `python main.py --symbol RELIANCE --backfill-years 5 --max-jobs 20` to ingest; `python main.py --report` for coverage. Existing GitHub secrets are reused by the weekday workflow. Upload all files including hidden .github directory. Run the workflow manually once. Existing database capacity may prevent ingestion; do not delete legacy data without a tested backup.

## Queries
```sql
SELECT * FROM vw_fund_pnl_statement WHERE symbol='RELIANCE' ORDER BY period_end DESC;
SELECT * FROM vw_fund_balance_sheet WHERE symbol='HDFCBANK';
SELECT * FROM vw_fund_cash_flow WHERE symbol='INFY';
SELECT * FROM vw_fund_financial_revisions WHERE symbol='TATASTEEL';
```
Monetary pivot columns are crore, EPS is rupees/share. Candidate views are unverified; conflicting values produce NULL rather than arbitrary selection. Exact decimal strings are stored. Verified view is intentionally empty. Original documents and rejected facts are compressed in local archive; workflow artifacts expire after 14 days and are not durable backups.

## Limitations / acceptance blockers
Live NSE and Turso were not tested. Endpoint parameters and mappings are candidates, not verified taxonomy definitions. Strict ISIN matching can reject filings with other entity identifiers. PDF discovery only, no PDF extraction or independent reconciliation, BSE acquisition, full taxonomy mapping, durable archival, backup/restore, reset or legacy migration. Accounting checks cover selected identities, not full statement verification. No guaranteed five-year completeness. Queue is basic and can retry unsuccessful recent jobs before historical work; parsed jobs require explicit requeue for revision discovery. Storage guard uses page count, not provider account quota; no atomic per-filing transaction on remote writes. Watchlist is fixed; --watchlist is accepted for CLI compatibility only. No NBFC pilot. No claims of production readiness. The pilot gate stays closed.

### Acquisition correction
The default API host is the previously used official www.nseindia.in endpoint. No mandatory homepage request is made. A 401, 403 or 429 stops the source for the run; no alternate host is attempted to circumvent a denial. The workflow first runs a read-only Reliance audit (no database connection). If no facts are acquired, it exits 2 with JSON diagnostics and skips ingestion. Subsequent scheduled runs retry. The audit and ingestion reports include HTTP status, content type and request parameters. Existing storage guard remains active. BSE fallback and independent PDF verification are not implemented. Live accessibility is not established by the mocked tests.
