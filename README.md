# Lancer Nexus Events

The Events repository defines large-scale event and battle workflows for Lancer Nexus.

## Responsibilities

- Event definitions and schedules
- Registration and eligibility
- Reserved event-server lifecycle
- Participant and group admission
- Event start, lock, result and return-transfer states
- Event-specific metrics and audit records

Event logic integrates with the Gateway and Coordinator but does not replace the authoritative game-server simulation.

## Shared Protocol

The shared contracts are checked out in the `Protocol` submodule. Update it before local builds with:

```bash
git submodule update --init --remote --merge Protocol
```

## Current service slice

`Events.sln` contains a .NET 10 API and tests. The API stores event definitions, participant registrations and revision-checked lifecycle transitions in MySQL. Registration locks the event row before checking its phase, time window, per-event account/character uniqueness and participant capacity. Registration and lifecycle requests persist their decisions, idempotency keys and audit records transactionally. Internal callers use configured bearer credentials (`Events:ServiceApiKeys:<service-id>`, each at least 32 characters); only `administration` or `events-admin` can create definitions, registration requests must come from Gateway, and account-attributed lifecycle requests must come from Gateway.

Apply migrations `001_events_lifecycle.sql`, `002_event_registrations.sql` and `003_event_server_assignments.sql` to the configured Events database before starting the API. Configure `ConnectionStrings:Events` (or `Events:MySqlConnectionString`) and service keys through protected deployment configuration. `/health/ready` checks that all required tables exist. Set `Events:Coordinator:BaseUrl` to the private HTTPS Coordinator origin and `Events:Coordinator:ApiKey` to its internal bearer key. The client verifies the Coordinator service, Protocol version and `event_server_reservations_v1` capability before its first request. Redirects are disabled and calls time out after eight seconds. Capabilities include `event_server_assignment_v1` and `event_reservation_recovery_v1`.

The lifecycle transition into `Provisioning` creates a deterministic, idempotent Coordinator reservation for the event revision, system, participant capacity and time window. An event cannot move from `Provisioning` to `Ready` until the reservation exists in both services. A recovery worker retries provisioning reservations that were not persisted locally and releases them only after the durable event phase becomes `Cancelled`, `Failed` or `Closed`. Coordinator and Events use separate stores, so retries reconcile uncertain HTTP outcomes; this is not a distributed SQL transaction.

The registration endpoint trusts the authenticated Gateway service to attest that the live account session and character lease were checked. Gateway's `POST /api/v1/events/{eventId}/registrations` currently validates the signed session token, active MySQL session and current character lease before forwarding the account, session and lease version over HTTPS. This cross-service path has unit and database coverage but no live Gateway-to-Events deployment smoke yet. Coordinator assignment now persists the selected instance and endpoint in Events; authoritative instance result acceptance and return-transfer orchestration remain open. The service does not modify normal credits or inventory.

Run the service checks with:

```bash
dotnet restore Events.sln
dotnet format Events.sln --verify-no-changes --no-restore
dotnet build Events.sln --configuration Release --no-restore --warnaserror -p:UseSharedCompilation=false
LANCER_NEXUS_EVENTS_TEST_MYSQL='Server=127.0.0.1;Port=3306;User ID=root;Database=mysql' dotnet test Events.sln --configuration Release
```

The MySQL tests create and drop randomly named databases on the configured test server. Use a disposable database server. CI starts an isolated MySQL 8.0 service and runs service and Protocol checks, including registration capacity, durable event assignments and release recovery. It does not run a live multi-process Coordinator/Events deployment smoke, assigned-instance result acceptance or transfers.
