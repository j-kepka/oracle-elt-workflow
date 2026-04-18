# Public Roadmap

This document is the simplified public roadmap for the repository.
It is intentionally shorter and less detailed than the internal planning notes.
Phase labels below describe delivery stages rather than GitHub pull request numbers.
The published sequence is kept contiguous; if an earlier placeholder idea is merged or deferred, later public phase numbers are normalized instead of leaving gaps.

## Scope Boundary

For portfolio/demo purposes, the practical MVP scope ends after `Phase-07`.

- `Phase-01` -> `Phase-07`: practical MVP scope
- `Phase-08`: `MVP+`
- `Phase-09`: `MVP++`

This means the repository can be considered demo-complete without implementing every later hardening item planned beyond `Phase-07`.

## Current Status

Completed:
- `Phase-01`: baseline `external -> stage -> core` flow
- `Phase-02`: `business_date`, dated files, validation, reject handling
- `Phase-03`: `.ok` ready files, `AUTO` / `MANUAL`, cutoff handling, current-status control table
- `Phase-04`: join-ready `clients` snapshot, same-day FK from transfers to clients, extra demo AML helper fields, duplicate-to-reject handling
- `Phase-05`: repeatable matrix-based smoke flow with deterministic compare helpers

Current public scope:
- `Phase-06 Part 1`: AML-oriented input extension on `clients` and `client_transfers`, manual `ref_fx_rate_daily`, and dedicated AML demo fixture/validation helpers
- completed pre-mart stabilization fix: loader runtime cleanup before `Phase-06 Part 2`, including query-scoped external-file binding and bounded `AUTO` retry behavior for missing `.ok`
- current pre-mart file-layout fix: split local file areas into `extdata/inbound`, `extdata/outbound`, and `extdata/work` before the mart/export path

Planned next inside MVP:
- `Phase-06 Part 2`: `mart_transfer_aml`, `amount_eur`, and first AML review flags / reason codes
- `Phase-07`: export/spool flow

Optional after MVP:
- `Phase-08`: lightweight inbound hardening, mainly reference dictionaries replacing hardcoded validation lists
- `Phase-09`: richer ETL run logs, DQ observability, and selected operational hardening

## What The MVP Already Demonstrates

- Oracle external-table based file ingestion
- `stage`, `reject`, and `core` layers
- synthetic dated snapshots driven by `business_date`
- `.ok` file contract and source-to-target reconciliation
- current-status process control in `CTL_PROCESS_RUN`
- `MANUAL` and `AUTO` modes
- same-day join between transfer and client snapshots
- a client snapshot reporting-state flag with `ACTIVE` / `ARCHIVED`
- duplicate business keys rejected before the `core` refresh
- AML-oriented client context fields flowing through the `clients` pipeline
- `transfer_title` flowing through the `client_transfers` pipeline
- manual `ref_fx_rate_daily` support for later EUR normalization
- dedicated AML demo dataset and validation helpers
- rebuild-based smoke testing for success, warning, and failure cases

## Notes

- This roadmap is intentionally public-facing and stable.
- Detailed task breakdown, internal tradeoffs, and private planning remain in internal docs.
- The project is a local demo/sandbox portfolio project, not a production deployment baseline.
- Input files and database records are synthetic demo data; fields that may resemble banking or customer identifiers are placeholders only.
- Deep real-world validation, such as IBAN checksum rules or identifier checksum validation, is intentionally out of scope for the demo unless explicitly called out by a later hardening phase.
