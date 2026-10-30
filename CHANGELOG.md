# Changelog

All notable changes to this project will be documented in this file.

## [v2.0.237] - 2026-10-30

- Fix `:uk.gov.ons.nhspd/PCT_ORG`: nil outside England, Wales and the Isle of Man, where NHSPD's PCT is not an ODS code, and nil when ODS has no such organisation; `:uk.gov.ons.nhspd/CTRY` is now resolved from a postcode
- Breaking: `:uk.gov.ons.nhspd/PCT_ORG` now requires `:uk.gov.ons.nhspd/CTRY` as well as `:uk.gov.ons.nhspd/PCT`, so needs an NHSPD index with the CTRY column (as in the default profile); without a sub-query it returns the full organisation rather than only its identifier

## [v2.0.234] - 2026-01-30

- Add `uk-org->fhir-telecom` resolver for `:org.hl7.fhir.Organization/telecom` (placeholder; returns nil)

## [v2.0.233] - 2026-01-15

- Add `stream-all-orgs` for lazy iteration over all organisations using a reducing function

## [v2.0.232] - 2025-11-13

- Add optional connection pooling using HikariCP, via `:pool` in `open-index`

## [v2.0.231] - 2025-11-11

- Add `fetch-orgs` batch function for fetching multiple organizations efficiently
- Optimize batch functions to reuse database connections, eliminating connection open/close overhead
- Performance improvements: fetch-orgs now 9.3x faster than N+1 approach (17.5ms → 1.88ms for 20 orgs)

## [v2.0.229] - 2025-11-10

- Add `org-codes->active-successors` batch function for finding active successors of multiple organizations
- Add critical performance indices on succession table (predecessor_org_code, successor_org_code)
- Batch function eliminates N+1 query problem for multiple organizations (4-10x faster)
- Combined optimizations provide up to 14x performance improvement
- Add benchmark tool (clj -M:bench) to validate succession query performance

## [v2.0.228] - 2025-11-09

- Add `org-code->active-successors` for finding active successor organizations

## [v2.0.226] - 2025-07-27

* Add 'os-grid-reference' for any postcode or postcode prefix to top-level API for convenience

## [v2.0.224] - 2025-07-24

- Migrate from Lucene to SQLite
- Improvements to import and FHIR r4 representation
- Significant fixes to graph API
- Upgrade to latest 'nhspd' which also uses SQLite backing store
 
## [v1.2.207] - 2025-01-10

- Fix comment code

## [v1.2.206] - 2025-01-10

- Switch to EPL and include license in pom

## [v1.2.205] - 2025-01-09

- Add child-orgs, equivalent-org-ids and equivalent-and-child-org-ids
- Add organisational relationships into searchable index
- Tidy, and bump dependencies
- Bump to latest trud
- Avoid reflection
- Improve docstring
- Fix type hints
- Fix docstring
- Fix indent
- Fix sort based on location

## [v1.1.195] - 2022-05-31

- Upgrade to nhspd v1.1.40

## [v1.1.194] - 2022-05-31

- Add resolver for 'isPartOf'

## [v1.1.193] - 2022-05-31

- Add convenience resolver for isOperatedBy relationship
- Fix well known namespace for organisational id