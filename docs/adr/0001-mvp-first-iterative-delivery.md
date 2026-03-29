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
4. Then add `.ok` readiness checks, current-status logging, and a small cutoff rule.
5. Then add business transformations.

## Why this approach
- Early visible progress.
- Easier review in small PRs.
- Lower risk of over-engineering at the start.

## Current note
Current setup runs in local `dev/sandbox`.
Current scope supports dated CSV loads for one selected `business_date`, a matching `.ok` ready file, visible rejects, and one current process-status row per `business_date`.
Advanced hardening is planned for later PRs.
This MVP is not a production deployment baseline; any production-like reuse requires a separate security and infrastructure review.

## Rough public PR map
This project is being built in a few small PRs.

- `PR-01` is the first simple flow: external table -> stage -> core, plus a basic scheduler job.
- `PR-02` adds `business_date`, dated input files, basic validation, and a reject table.
- `PR-03` adds a small control file like `.ok`, a simple current-status table, and a cutoff rule for `AUTO` mode.
- File archiving and fuller run logs can still move to later PRs if needed.
- After that, business transformations can be added in a separate step.

This is only a rough public map.
Some details may still move a bit between later PRs.
