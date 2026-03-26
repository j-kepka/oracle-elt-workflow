# ADR-0000: Public Project Scope

- Status: Accepted
- Date: 2026-03-25

## Goal
Build a simple Oracle ELT workflow and show steady progress in small PRs.

## Environment
For now we keep one simple environment: `dev/sandbox` (local).

## In Scope (MVP)
- Load flat file with Oracle external table.
- Move data external -> stage -> core.
- Run load manually (procedure) and optionally by scheduler.

## Out of Scope (for now)
- Production secrets and infrastructure details.
- Full runbook and private planning notes.
- Enterprise-level hardening in MVP phase.

## Public Source of Truth
- README + ADR files in `docs/adr/`.
- Internal working notes stay in `docs/internal/`.
