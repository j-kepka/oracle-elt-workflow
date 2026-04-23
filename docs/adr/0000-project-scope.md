# ADR-0000: Public Project Scope

- Status: Accepted
- Date: 2026-03-25
- Last reviewed: 2026-04-22

## Context
A small, inspectable Oracle ELT project is needed for portfolio/demo use.
The project should demonstrate batch ingestion, validation, reject handling, process status tracking, and an AML-oriented downstream path without becoming a production template.

## Decision
The public project scope is a local `dev/sandbox` Oracle ELT workflow.
The practical MVP ends after `Phase-07`, when the flow reaches `client + transfer -> AML mart -> spool`.

MVP scope includes:
- Load dated CSV files with Oracle external table.
- Use a small `.ok` ready file for inbound readiness and row-count checks.
- Run one selected `business_date`.
- Validate rows and keep rejects visible.
- Move data external -> stage -> core.
- Track one current process status row per `process_name` and `business_date`.
- Build an AML-oriented mart and final spool/export path by the end of `Phase-07`.

## Rationale And Consequences
- The scope stays small enough to inspect in a local sandbox.
- Public documentation can present a stable demo story without exposing working notes or private planning.
- The project remains a portfolio/demo baseline only.
- Any production-like reuse requires separate security, infrastructure, operations, and data-governance review.

## Scope Boundary
- `Phase-01` -> `Phase-07`: practical MVP.
- `Phase-08`: optional `MVP+` inbound hardening.
- `Phase-09`: optional `MVP++` observability and operations polish.
- Production deployment, production secrets, production infrastructure, and full operational hardening are outside the MVP.
