# ADR-0000: Public Project Scope

- Status: Accepted
- Date: 2026-03-25

## Goal
Build a small Oracle ELT workflow and grow it in small PRs.

## Environment
Current environment: local `dev/sandbox`.

## In Scope (MVP)
- Load dated CSV files with Oracle external table.
- Run one selected `business_date`.
- Validate rows and keep rejects visible.
- Move data external -> stage -> core.
- Run load manually (procedure) and optionally by scheduler.

## Out of Scope (for now)
- Production secrets and infrastructure details.
- Private runbooks and planning notes.
- Full production hardening in MVP phase.

## Public Source of Truth
- README + ADR files in `docs/adr/`.
- Internal working notes stay in `docs/internal/`.
