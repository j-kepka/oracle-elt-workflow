# ADR-0000: Public Project Scope

- Status: Accepted
- Date: 2026-03-25

## Goal
Build a small Oracle ELT workflow and grow it in small delivery phases.

## Environment
Current environment: local `dev/sandbox`.

## In Scope (MVP)
- Load dated CSV files with Oracle external table.
- Use a small `.ok` ready file for inbound readiness and row-count checks.
- Run one selected `business_date`.
- Validate rows and keep rejects visible.
- Move data external -> stage -> core.
- Track one current process status row per `business_date`.
- Run load manually (procedure) and optionally by scheduler.

## Out of Scope (for now)
- Production secrets and infrastructure details.
- Private runbooks and planning notes.
- Full production hardening in MVP phase.
- Any production-like reuse without a separate security and infrastructure audit.

## Public Source of Truth
- README + ADR files in `docs/adr/`.
