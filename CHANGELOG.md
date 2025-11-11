# Changelog

All notable changes to this project will be documented in this file.

## [Not yet released]

- Add `fetch-orgs` batch function for fetching multiple organizations efficiently (~20% faster than N+1)

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