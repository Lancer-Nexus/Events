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

## Working-model escalation

- If a task requires complex reasoning beyond the current model's reliable scope, ask the user whether switching to a stronger model is desired before continuing.
- Do not switch models silently or broaden the task because a stronger model may be useful.

## Verification

Test registration races, event cancellation, late joins, full capacity, server failure, reconnects and return transfers.

The current service solution is `Events.sln` (API plus tests). Build with `dotnet build Events.sln -c Release --no-restore --warnaserror -p:UseSharedCompilation=false`; run the full service tests against a disposable MySQL 8 server by setting `LANCER_NEXUS_EVENTS_TEST_MYSQL`. The MySQL fixture creates and drops an isolated random database and applies migrations in order. Gateway currently validates the session and lease before forwarding registration, but the live cross-service path has not been smoke-tested. Provisioning uses a durable Coordinator server assignment and a recovery worker; assigned-instance result acceptance and return-transfer support remain unimplemented.
