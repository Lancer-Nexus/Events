# AGENTS.md – Lancer Nexus Events

## Mission

Provide predictable lifecycle management for scheduled events and large battles.

## MVP architecture baseline

- Events owns event registration and results, while Coordinator owns capacity reservations, Gateway owns identity and the active game instance owns live simulation.
- Event entry and return use the idempotent transfer lifecycle `Requested -> Reserved -> Prepared -> SourceFrozen -> TargetAccepted -> Committed -> SourceReleased`; the source remains authoritative until the MySQL lease commits.
- Character authority is protected by the current MySQL `lease_version` fencing token. Redis distributes transient event/chat state and is not authoritative storage.
- Event messages use versioned `Protocol` contracts and capability negotiation.

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
