# Public Roadmap

This document is the simplified public roadmap for the repository.
It is intentionally shorter and less detailed than the internal planning notes.
Phase labels below describe delivery stages rather than GitHub pull request numbers.
The published sequence is kept contiguous; if an earlier placeholder idea is merged or deferred, later public phase numbers are normalized instead of leaving gaps.

## Scope Boundary

For portfolio/demo purposes, the practical MVP scope ends before `Phase-09`.

- `Phase-01` -> `Phase-08`: demo-facing scope
- `Phase-09`, `Phase-10`, `Phase-11`, `Phase-12`: optional post-MVP work

This means the repository can be considered demo-complete without implementing every later hardening item.

## Current Status

Completed:
- `Phase-01`: baseline `external -> stage -> core` flow
- `Phase-02`: `business_date`, dated files, validation, reject handling
- `Phase-03`: `.ok` ready files, `AUTO` / `MANUAL`, cutoff handling, current-status control table
- `Phase-04`: join-ready `clients` snapshot, same-day FK from transfers to clients, extra demo AML helper fields, duplicate-to-reject handling

Planned next inside MVP:
- `Phase-05`: AML data harmonization prerequisites
- `Phase-06`: first AML mart plus simple sequential orchestration
- `Phase-07`: export/spool flow

Optional within MVP:
- `Phase-08`: lightweight inbound hardening, mainly reference dictionaries replacing hardcoded validation lists, stricter `.ok` contract enforcement (exactly one line), and safer dataset-level concurrency for external-table `LOCATION` switching

Optional after MVP:
- `Phase-09`: richer ETL run logs and DQ observability
- `Phase-10`: historized pipeline direction and delayed reload hardening
- `Phase-11`: optional scheduling/dispatcher extensions
- `Phase-12`: cleanup and extra hardening outside the main delivery flow

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
- six demo AML helper fields already flowing through the `clients` pipeline
- rebuild-based smoke testing for success, warning, and failure cases

## Notes

- This roadmap is intentionally public-facing and stable.
- Detailed task breakdown, internal tradeoffs, and private planning remain in internal docs.
- The project is a local demo/sandbox portfolio project, not a production deployment baseline.
- Input files and database records are synthetic demo data; fields that may resemble banking or customer identifiers are placeholders only.
- Deep real-world validation, such as IBAN checksum rules or identifier checksum validation, is intentionally out of scope for the demo unless explicitly called out by a later hardening phase.
