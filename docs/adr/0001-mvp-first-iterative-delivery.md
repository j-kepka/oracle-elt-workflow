# ADR-0001: MVP-First Delivery

- Status: Accepted
- Date: 2026-03-25

## Context
A working baseline should be delivered quickly, then improved step by step.

## Decision
MVP-first delivery is applied, followed by small weekly PRs.

Order:
1. Working external -> stage -> core flow.
2. Basic scheduler run.
3. Then add validations, rejects, and logging.
4. Then add business transformations.

## Why this approach
- Early visible progress.
- Easier review in small PRs.
- Lower risk of over-engineering at the start.

## Current note
MVP runs in one simple `dev/sandbox` setup.
Advanced hardening is planned for later PRs.
