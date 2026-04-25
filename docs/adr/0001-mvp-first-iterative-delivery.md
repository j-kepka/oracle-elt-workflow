# ADR-0001: MVP-First Delivery

- Status: Accepted
- Date: 2026-03-25
- Last reviewed: 2026-04-23

## Context
A working demo is needed early, then improved through small, reviewable delivery phases.
The project is intentionally scoped as a local Oracle ELT sandbox rather than a production deployment baseline.

## Decision
Delivery follows an MVP-first sequence:
1. Establish a working `external -> stage -> core` flow.
2. Add `business_date`, dated files, validation, rejects, and `.ok` readiness checks.
3. Add a join-ready `clients` snapshot beside `client_transfers`.
4. Add repeatable smoke validation.
5. Add AML-oriented input context, mart logic, first review classification, and export/spool output.

## Rationale And Consequences
- Early phases keep the repository runnable and easy to inspect.
- Each phase adds one visible capability instead of a broad unfinished platform.
- The design can show batch-processing discipline without requiring a full orchestration framework.
- Operational hardening is added only where it supports the demo scope.

## Scope Boundary
The practical MVP ends after `Phase-07`, with a thin `load -> mart -> spool` flow.
`Phase-08` is reserved for optional `MVP+` inbound hardening and selected maintainability polish.
`Phase-09` is reserved for optional `MVP++` observability and operations polish.

AML, GIIF-like naming, and Polish-domain references are simplified demo context only.
They are not legal guidance, regulatory interpretation, or a claim of production compliance.

Plain tables are sufficient for the current synthetic datasets.
Date-based partitioning and deeper operational hardening are deferred until real volume, reuse, or post-MVP scope justifies them.
