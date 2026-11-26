# Fundamental_ind — consolidated ingestion update

Upload all files including `.github/workflows/pipeline.yml` to main. Existing TURSO_DATABASE_URL and TURSO_AUTH_TOKEN secrets are used. Push and four-hour schedule run tests, additive schema initialization, universe refresh, ingestion and coverage reporting. No legacy tables are dropped. The old pipeline remains for compatibility; the scheduled workflow uses reliable.py. New jobs are intentionally seeded independently because legacy success did not prove extraction.

## Local commands
`python -m pytest -q`; `python reliable.py init --local test.db`; `python reliable.py report --local test.db`; `python reliable.py export --local test.db --isin ISIN` (optional `--as-of YYYY-MM-DD`). Omit --local only for authenticated production use. Reports are temporary workflow artifacts, not backups.

## Reliability and limitations
NSE legacy/integrated, ownership, actions, announcements, annual report and BSE results/announcements discovery candidates are configured in reliable.py. BSE listings require exact ISIN matches. These endpoints and response schemas have NOT been live verified; network DNS was unavailable. BSE pagination, relative attachments and exchange-specific date/scope mappings remain incomplete. Empty responses are explicitly tracked, not counted as parsed. PDF is metadata only. XBRL numeric facts retain exact strings, dates, units, dimensions and source hashes. Validated means basic structural checks only, not accounting or independent verification. Unknown scope is excluded from default export. Entity identity within XBRL is not independently verified. No facts are certified verified. As-of export only uses recognized ISO publication dates and does not select latest restatement. Source documents are not permanently archived. Cross-exchange reconciliation, accounting checks, quota enforcement, backup/restore, inline XBRL, PDF extraction, derived ratios and full coverage remain incomplete. Existing facts are preserved but not migrated into the new validated view. New queue uses year windows, not proof of period completeness. Reports show actual counts when run against production; local tests use synthetic fixtures. No production data or remote workflow was verified here.

### NSE route repair
Integrated financial discovery uses `/api/integrated-filing-results` with type and pagination; legacy discovery explicitly requests reporting periods. A one-time migration requeues affected unsuccessful NSE jobs. Integrated years before 2025 are marked not applicable. Annual report discovery uses symbol and filters by ending year, but records lacking company identity remain excluded. Sources: https://github.com/jugaad-py/jugaad-data/blob/master/jugaad_data/nse/live.py and https://bennythadikaran.github.io/NseIndiaApi/api.html . Live runner verification remains necessary; financial facts are not independently certified.

## Coverage-driven corrective build

Includes strict SQL parameter binding, financial-first task allocations, exclusion of pre-2025 integrated jobs, additional exact identifier aliases, and diagnostics for unattributed records. Coverage reports distinguish ownership from financial facts, include all active companies, legacy dataset counts, source/job health, document issues, candidate statement periods, and an estimated database size. Candidate statements are not certified complete. The configurable `DATABASE_BYTE_BUDGET` defaults to 400 MB as a conservative application decision, not an official Turso limit; page estimates may be unavailable remotely. The worker warns at 70%, pauses historical work at 95%, and refuses work at 100% at batch start. This is not account-wide quota enforcement.

Local tests use synthetic fixtures. Live NSE/BSE endpoints, BSE mappings, production database counts, accounting reconciliation, independent verification, PDF extraction, and permanent backups remain unverified or incomplete. No production data has been modified by local testing. The existing source adapters remain candidates; diagnostics are intended to reveal response field mismatches without attributing unknown records to a company.

## Five-company pilot (validation foundation)
The scheduled worker only processes pilot_members (RELIANCE, INFY, HDFCBANK,
TATASTEEL, HINDUNILVR), starting with the calendar year containing the first
of five completed Indian fiscal years. Existing jobs and facts are retained.
Calendar-year discovery can include extra months; validation reports filter by
period end. The worker stops starting jobs at 95% of the configured page budget
or when usage cannot be measured; a single job can still overshoot. No data is
deleted. dbstat storage reporting is best effort.

Reports include pilot_acceptance.json, validation_summary.json,
company_quality_score.csv, accounting_checks.csv, financial_reconciliation.csv,
missing_periods.csv and table_storage.csv. Accounting checks are diagnostic
candidate checks on exact local concept names within a filing; cross-exchange
comparisons do not establish filing-version equivalence. Missing quarters are
candidate gaps, not proof of unavailable filings. Expansion is deliberately
locked until independent PDF verification, taxonomy mapping, version matching,
backup restoration and storage scalability are implemented and demonstrated.
No fact is certified by this release. Existing v2_financial_data remains a
structural-validation view, not a verified analytics view.
