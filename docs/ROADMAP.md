# Public Roadmap

This document is the public phase snapshot for the repository.
Phase labels describe delivery stages rather than publication or review numbers.
The published sequence is kept contiguous so the public roadmap remains easy to read.

## Scope Boundary

This repository is a local demo/sandbox portfolio project, not a production deployment baseline.
For portfolio/demo purposes, the practical MVP scope ends after `Phase-07`.

- `Phase-01` -> `Phase-07`: practical MVP scope
- `Phase-08`: `MVP+` post-MVP inbound hardening and selected maintainability polish
- `Phase-09`: `MVP++` optional observability and operations polish

The repository can be considered demo-complete at MVP level after `Phase-07`.
Later hardening and operational polish remain optional extensions.

## Current Status

Completed:
- `Phase-01`: baseline `external -> stage -> core` flow
- `Phase-02`: `business_date`, dated files, validation, reject handling
- `Phase-03`: `.ok` ready files, `AUTO` / `MANUAL`, cutoff handling, current-status control table
- `Phase-04`: join-ready `clients` snapshot, same-day FK from transfers to clients, extra demo AML helper fields, duplicate-to-reject handling
- `Phase-05`: repeatable matrix-based smoke flow with deterministic compare helpers
- `Phase-06 Part 1`: AML-oriented input extension on `clients` and `client_transfers`, manual `ref_fx_rate_daily`, and dedicated AML demo fixture/validation helpers
- `Phase-06 Part 2`: review-ready AML mart with `mart_transfer_aml`, `amount_eur`, FX coverage checks, first AML review flags and reason codes, and `report_type_candidate`
- supporting runtime and file-layout stabilization for the mart/export path

Planned next inside MVP:
- `Phase-07`: export/spool flow

Optional after MVP:
- `Phase-08`: lightweight inbound hardening, mainly reference dictionaries replacing hardcoded validation lists, plus selected maintainability polish such as a shared PL/SQL utility package
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
- manual `ref_fx_rate_daily` support for EUR normalization
- first AML mart with `mart_transfer_aml`, `amount_eur`, and first review-oriented classification fields
- first AML review flags, reason codes, and `report_type_candidate`
- FX coverage validation for the AML mart build
- dedicated AML demo dataset and validation helpers
- rebuild-based smoke testing for success, warning, and failure cases

## Notes

- This roadmap is intentionally public-facing and stable.
- Detailed task breakdown, delivery tradeoffs, and private planning are outside this public roadmap.
- The project is a local demo/sandbox portfolio project, not a production deployment baseline.
- Input files and database records are synthetic demo data; fields that may resemble banking or customer identifiers are placeholders only.
- Deep real-world validation, such as IBAN checksum rules or identifier checksum validation, is intentionally out of scope for the demo unless explicitly called out by a later hardening phase.
