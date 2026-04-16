# ADR-0001: MVP-First Delivery

- Status: Accepted
- Date: 2026-03-25

## Context
A working version is needed early, then improved step by step.

## Decision
MVP-first delivery is used, followed by small delivery phases that can be published through separate GitHub PRs.

Order:
1. Working external -> stage -> core flow.
2. Basic scheduler run.
3. Then add `business_date`, dated input files, validations, and rejects.
4. Then add `.ok` readiness checks, current-status logging, and a small cutoff rule.
5. Then add business transformations.

## Why this approach
- Early visible progress.
- Easier review in small increments.
- Lower risk of over-engineering at the start.

## Current note
Current setup runs in local `dev/sandbox`.
Current scope supports dated CSV loads for `client_transfers` and `clients`, matching `.ok` ready files, visible rejects, one current process-status row per `process_name` and `business_date`, a same-day join from transfers to the client snapshot, and a simple client reporting status (`ACTIVE` / `ARCHIVED`) kept inside the snapshot model.
Phase labels in this ADR describe delivery stages rather than GitHub pull request numbers.
For portfolio/demo purposes, the practical MVP scope is treated as complete after `Phase-07`.
`Phase-08` is the current `MVP+` bucket and `Phase-09` is the current `MVP++` bucket.
Advanced hardening is planned for later optional phases.
This MVP is not a production deployment baseline; any production-like reuse requires a separate security and infrastructure review.

## Rough Public Phase Map
This project is being built in a few small delivery phases.

- `Phase-01` is the first simple flow: external table -> stage -> core, plus a basic scheduler job.
- `Phase-02` adds `business_date`, dated input files, basic validation, and a reject table.
- `Phase-03` adds a small control file like `.ok`, a simple current-status table, and a cutoff rule for `AUTO` mode.
- `Phase-04` extends the same load pattern to a first join-ready `clients` snapshot alongside `client_transfers`.
- `Phase-05` adds a repeatable matrix-based smoke flow for the current load baseline.
- `Phase-06` extends the current input model toward AML and builds the first AML review-ready mart in two smaller steps.
- `Phase-07` adds the final spool/export step and the thin sequential `load -> mart -> spool` closeout for practical MVP completion.
- `Phase-08` can be used for light inbound hardening, mainly small reference dictionaries replacing hardcoded validation lists.
- `Phase-09` is optional post-MVP work around richer ETL observability and selected operational hardening.

This is only a rough public map.
Some details may still move a bit between later PRs.
