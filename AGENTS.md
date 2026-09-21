# AGENTS.md – Lancer Nexus Events

## Mission

Provide predictable lifecycle management for scheduled events and large battles.

## Rules

- Model event lifecycle as explicit state transitions.
- Make registration, admission and start operations idempotent.
- Reserve capacity before confirming participation.
- Define what happens to players when an event server fails or reaches capacity.
- Preserve group and formation membership where the event rules permit it.
- Store event results and audit data durably.
- Do not trust client-provided event scores, permissions or participant state.

## Verification

Test registration races, event cancellation, late joins, full capacity, server failure, reconnects and return transfers.
