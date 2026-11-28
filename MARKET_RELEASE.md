# Market tracking candidate

Additive market tables, independent MARKET workflow, reuse of existing compressed Yahoo price payloads, 400-calendar-day initial fetch and seven-day overlapping refresh, indexed company/date/source storage. No historical financial or membership records are deleted. Existing five-year payloads remain intact. Yahoo series are unverified and aliases are not live validated. Raw close returns are trading-observation based (5/21/63/126/252 observations), not calendar returns or total returns. Adjusted close is stored separately; corporate action revisions outside the overlap are not reconciled. Volume is shares, not turnover. Index volume is deliberately null. Current-membership breadth is a retrospective proxy, not official index history. Factor indices without a configured Yahoo alias are explicitly unavailable; their constituent breadth is still calculable once company data is present. No official factor index acquisition is implemented. Coverage counts observations, not exchange-calendar completeness.

## Deployment
Replace repository contents with archive including .github. Existing secrets remain unchanged. MARKET runs six times daily in shared concurrency group, up to 100 entities per run, with one-day retry and seven-day retry for unmapped indices. CORE/EXTENDED schedules reduced to four runs daily each; MEMBERSHIP once daily. Existing storage guards retained. Provider read/write quota accounting is not implemented; check remaining quota before enabling schedules. No production execution performed. This is a locally tested candidate, not a verified production release.

## Queries (filter by symbol/date)
```sql
SELECT * FROM vw_company_daily_prices WHERE symbol='COALINDIA' AND trading_date>=date('now','-1 year') ORDER BY trading_date;
SELECT trading_date,close,volume,daily_return_pct,relative_volume FROM vw_company_market_performance WHERE symbol='COALINDIA' ORDER BY trading_date DESC LIMIT 260;
SELECT * FROM vw_sector_daily_performance WHERE code='NIFTY_BANK' ORDER BY trading_date DESC LIMIT 260;
SELECT * FROM vw_factor_breadth WHERE trading_date>=date('now','-1 year');
SELECT * FROM vw_market_data_coverage;
SELECT * FROM vw_market_acquisition_progress;
```
Window-based analytics can be expensive on remote full-universe queries; use bounded queries and local processing. No quota reduction measurement or complete historical data guarantee. Tests use synthetic data only.

## Recovery correction
Existing MARKET schedule automatically prioritizes up to five due failed entities per run, then ordinary work, then remaining due failures. Detailed per-entity attempts, stage and redacted exception messages appear in market/summary.json. Database failures stop the batch rather than being classified as provider failures. Transient download exceptions receive at most three attempts. Invalid cached payloads fall back to Yahoo; existing rows and ticker mappings are preserved. Unsupported indices remain explicit. No live provider or Turso validation performed; alternate ticker mappings are not guessed. No schema reset or new secrets. Deploy the complete archive including .github. Existing scheduled MARKET runs pick up the correction automatically. Tests use local SQLite and simulated provider responses.

### Recovery hotfix: Incorrect padding
Production SAILIFE failed at PAYLOAD_LOOKUP before download. The remote adapter previously required padded Base64; it now restores missing padding and validates the alphabet. Market cached payload retrieval uses SQLite hex() and decodes inside the existing cache-parse fallback. Genuine database failures still stop acquisition. Existing retry timestamps are preserved. No database migration or reset. 24 local tests passed, including mocked unpadded remote BLOB responses; live recovery remains unverified.

## Full market execution
Workflow MARKET limit increased to 1000. Existing due dates, storage guards and 55-minute job timeout remain unchanged. Live full-universe execution not verified.

## Sector and factor source update
17 new Yahoo aliases (8 user supplied, 9 inferred). Existing mappings preserved. New aliases require Yahoo INDEX quote type and matching symbol; this is not independent benchmark verification. Full index backfill requires at least 230 rows and complete OHLC; freshness tolerance seven calendar days. This is a heuristic, not exchange-calendar completeness. Newly configured indices bypass old unsupported-source retry timestamps. Existing scheduled MARKET workflow continues; optional `python market.py --index-only --limit 1000` limits acquisition to mapped indices. Four thematic/strategy fund proxies intentionally excluded from official index storage. No live Yahoo or Turso validation performed. Built from previous full-market ZIP because GitHub DNS failed.
