# ADR-0001: MVP-First Delivery

- Status: Accepted
- Date: 2026-03-25

## Context
A working version is needed early, then improved step by step.

## Decision
MVP-first delivery is used, followed by small PRs.

Order:
1. Working external -> stage -> core flow.
2. Basic scheduler run.
3. Then add `business_date`, dated input files, validations, and rejects.
4. Then add readiness checks and process logging.
5. Then add business transformations.

## Why this approach
- Early visible progress.
- Easier review in small PRs.
- Lower risk of over-engineering at the start.

## Current note
Current setup runs in local `dev/sandbox`.
Current scope supports dated CSV loads for one selected `business_date` and keeps invalid rows in a reject table.
Advanced hardening is planned for later PRs.

## Rough public PR map
This project is being built in a few small PRs.

- `PR-01` is the first simple flow: external table -> stage -> core, plus a basic scheduler job.
- `PR-02` adds `business_date`, dated input files, basic validation, and a reject table.
- `PR-03` should add a small control file like `.ok`, plus moving processed files to archive.
- It would also be good to add one simple process status/log table there, if it still stays small enough.
- If that part gets too messy, fuller logging or cutoff rules can be moved to a later PR.
- After that, business transformations can be added in a separate step.

This is only a rough public map.
Some details may still move a bit between later PRs.
