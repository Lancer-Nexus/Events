using LancerNexus.Events;
using LancerNexus.Protocol;
using MySqlConnector;
using Xunit;

namespace LancerNexus.Events.Tests;

public sealed class MySqlEventStoreTests
{
    [MySqlFact]
    public async Task DefinitionsAreIdempotentAndLifecycleRequestsAreDurable()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            var store = new MySqlEventStore(connectionString, TimeProvider.System);
            var definition = Definition("event-idempotency");
            Assert.Equal(EventDefinitionWriteStatus.Created,
                (await store.CreateEventAsync(definition, "administration")).Status);
            Assert.Equal(EventDefinitionWriteStatus.AlreadyExists,
                (await store.CreateEventAsync(definition, "administration")).Status);
            Assert.Equal(EventDefinitionWriteStatus.Conflict,
                (await store.CreateEventAsync(definition with { RulesetId = "alternate" }, "administration")).Status);

            var request = Transition(definition.EventId, "open-registration", NexusEventPhase.Planned,
                NexusEventPhase.Registration, 1);
            var applied = await store.TransitionAsync(request, "service:coordinator");
            Assert.Equal(EventLifecycleStatus.Applied, applied.Status);
            Assert.Equal(2, applied.CurrentRevision);
            Assert.Equal(NexusEventPhase.Registration, applied.CurrentPhase);
            Assert.True(applied.IsValid());

            var restartedStore = new MySqlEventStore(connectionString, TimeProvider.System);
            var replay = await restartedStore.TransitionAsync(request, "service:coordinator");
            Assert.Equal(applied, replay);

            var stale = await restartedStore.TransitionAsync(Transition(definition.EventId, "stale-cancel",
                NexusEventPhase.Planned, NexusEventPhase.Cancelled, 1) with
            { ReasonCode = "operator_cancelled" }, "service:coordinator");
            Assert.Equal(EventLifecycleStatus.Conflict, stale.Status);
            Assert.Equal(NexusEventPhase.Registration, stale.CurrentPhase);
            Assert.Equal(2, stale.CurrentRevision);

            var reusedKey = await restartedStore.TransitionAsync(request with
            {
                RequestId = Guid.NewGuid(),
                CorrelationId = Guid.NewGuid()
            }, "service:coordinator");
            Assert.Equal(EventLifecycleStatus.Conflict, reusedKey.Status);
            Assert.Equal("idempotency_key_reused", reusedKey.ErrorCode);

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var auditCount = connection.CreateCommand();
            auditCount.CommandText = "SELECT COUNT(*) FROM event_lifecycle_audit WHERE event_id = @event_id";
            auditCount.Parameters.AddWithValue("@event_id", definition.EventId);
            Assert.Equal(3L, Convert.ToInt64(await auditCount.ExecuteScalarAsync()));
        });
    }

    [MySqlFact]
    public async Task ConcurrentTransitionsAtOneRevisionOnlyApplyOnce()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            var store = new MySqlEventStore(connectionString, TimeProvider.System);
            var definition = Definition("event-race");
            Assert.Equal(EventDefinitionWriteStatus.Created,
                (await store.CreateEventAsync(definition, "administration")).Status);
            var open = Transition(definition.EventId, "race-open", NexusEventPhase.Planned,
                NexusEventPhase.Registration, 1);
            var cancel = Transition(definition.EventId, "race-cancel", NexusEventPhase.Planned,
                NexusEventPhase.Cancelled, 1) with
            { ReasonCode = "operator_cancelled" };

            var decisions = await Task.WhenAll(
                store.TransitionAsync(open, "service:coordinator"),
                store.TransitionAsync(cancel, "service:coordinator"));

            Assert.Single(decisions, decision => decision.Status == EventLifecycleStatus.Applied);
            Assert.Single(decisions, decision => decision.Status == EventLifecycleStatus.Conflict);
            var current = await store.GetEventAsync(definition.EventId);
            Assert.Equal(2, current!.Revision);
            Assert.Contains(current.Phase, new[] { NexusEventPhase.Registration, NexusEventPhase.Cancelled });
        });
    }

    [MySqlFact]
    public async Task RegistrationsAreIdempotentAndRespectCapacity()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            var definition = Definition("event-registration") with { MaximumParticipants = 1 };
            var store = new MySqlEventStore(connectionString,
                new FixedTimeProvider(definition.RegistrationOpensUtc.AddMinutes(30)));
            Assert.Equal(EventDefinitionWriteStatus.Created,
                (await store.CreateEventAsync(definition, "administration")).Status);
            Assert.Equal(EventLifecycleStatus.Applied,
                (await store.TransitionAsync(Transition(definition.EventId, "registration-open",
                    NexusEventPhase.Planned, NexusEventPhase.Registration, 1), "service:coordinator")).Status);

            var request = Registration(definition.EventId, Guid.NewGuid(), 9001, "register-first");
            var accepted = await store.RegisterAsync(request);
            Assert.Equal(EventRegistrationStatus.Accepted, accepted.Status);
            Assert.True(accepted.IsValid());
            Assert.Equal(2, accepted.EventRevision);

            var restartedStore = new MySqlEventStore(connectionString,
                new FixedTimeProvider(definition.RegistrationOpensUtc.AddMinutes(30)));
            Assert.Equal(accepted, await restartedStore.RegisterAsync(request));
            var sameAccount = await restartedStore.RegisterAsync(Registration(definition.EventId,
                request.AccountId, request.CharacterId, "register-same-account"));
            Assert.Equal(EventRegistrationStatus.AlreadyRegistered, sameAccount.Status);
            Assert.Equal(accepted.RegistrationId, sameAccount.RegistrationId);

            var full = await restartedStore.RegisterAsync(Registration(definition.EventId,
                Guid.NewGuid(), 9002, "register-second"));
            Assert.Equal(EventRegistrationStatus.CapacityFull, full.Status);

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var count = connection.CreateCommand();
            count.CommandText = "SELECT COUNT(*) FROM event_registrations WHERE event_id = @event_id";
            count.Parameters.AddWithValue("@event_id", definition.EventId);
            Assert.Equal(1L, Convert.ToInt64(await count.ExecuteScalarAsync()));
            count.CommandText = "SELECT COUNT(*) FROM event_registration_audit WHERE event_id = @event_id";
            Assert.Equal(3L, Convert.ToInt64(await count.ExecuteScalarAsync()));
        });
    }

    [MySqlFact]
    public async Task ConcurrentRegistrationsCannotExceedEventCapacity()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            var definition = Definition("event-registration-race") with { MaximumParticipants = 1 };
            var store = new MySqlEventStore(connectionString,
                new FixedTimeProvider(definition.RegistrationOpensUtc.AddMinutes(30)));
            Assert.Equal(EventDefinitionWriteStatus.Created,
                (await store.CreateEventAsync(definition, "administration")).Status);
            await store.TransitionAsync(Transition(definition.EventId, "registration-race-open",
                NexusEventPhase.Planned, NexusEventPhase.Registration, 1), "service:coordinator");

            var decisions = await Task.WhenAll(
                store.RegisterAsync(Registration(definition.EventId, Guid.NewGuid(), 9101, "capacity-a")),
                store.RegisterAsync(Registration(definition.EventId, Guid.NewGuid(), 9102, "capacity-b")));

            Assert.Single(decisions, decision => decision.Status == EventRegistrationStatus.Accepted);
            Assert.Single(decisions, decision => decision.Status == EventRegistrationStatus.CapacityFull);
            Assert.All(decisions, decision => Assert.True(decision.IsValid()));
        });
    }

    [MySqlFact]
    public async Task EventServerAssignmentAndTerminalReleaseRecoverySurviveStoreRestart()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            var store = new MySqlEventStore(connectionString, TimeProvider.System);
            var definition = Definition("event-assignment-recovery");
            Assert.Equal(EventDefinitionWriteStatus.Created,
                (await store.CreateEventAsync(definition, "administration")).Status);
            Assert.Equal(EventLifecycleStatus.Applied, (await store.TransitionAsync(Transition(definition.EventId,
                "assignment-registration", NexusEventPhase.Planned, NexusEventPhase.Registration, 1),
                "service:coordinator")).Status);
            Assert.Equal(EventLifecycleStatus.Applied, (await store.TransitionAsync(Transition(definition.EventId,
                "assignment-provisioning", NexusEventPhase.Registration, NexusEventPhase.Provisioning, 2),
                "service:coordinator")).Status);
            var provisioning = (await store.GetEventAsync(definition.EventId))!;
            var request = new EventServerReservationRequestV1
            {
                RequestId = Guid.NewGuid(),
                CorrelationId = Guid.NewGuid(),
                EventId = definition.EventId,
                EventRevision = provisioning.Revision,
                TargetSystemId = definition.TargetSystemId,
                MaximumParticipants = definition.MaximumParticipants,
                StartsUtc = definition.StartsUtc,
                EndsUtc = definition.EndsUtc,
                IdempotencyKey = "event-assignment-reserve"
            };
            var decision = new EventServerReservationDecisionV1
            {
                RequestId = request.RequestId,
                CorrelationId = request.CorrelationId,
                EventId = request.EventId,
                ReservationId = Guid.NewGuid(),
                Status = EventServerReservationStatus.Reserved,
                InstanceId = "li-01",
                SystemId = "li01",
                Endpoint = "https://li-01.internal:7443",
                ReservedCapacity = request.MaximumParticipants
            };

            Assert.Single(await store.GetEventsNeedingServerAssignmentAsync());
            Assert.True(await store.SaveEventServerAssignmentAsync(request, decision));
            Assert.True(await store.SaveEventServerAssignmentAsync(request, decision));
            Assert.Empty(await store.GetEventsNeedingServerAssignmentAsync());
            var restarted = new MySqlEventStore(connectionString, TimeProvider.System);
            var persisted = (await restarted.GetEventServerAssignmentAsync(definition.EventId))!;
            Assert.Equal(decision.ReservationId, persisted.Decision.ReservationId);
            Assert.Equal(provisioning.Revision, persisted.Request.EventRevision);

            Assert.Equal(EventLifecycleStatus.Applied, (await restarted.TransitionAsync(Transition(
                definition.EventId, "assignment-cancel", NexusEventPhase.Provisioning,
                NexusEventPhase.Cancelled, provisioning.Revision), "service:coordinator")).Status);
            var pendingRelease = Assert.Single(
                await restarted.GetTerminalEventsWithUnreleasedServerAssignmentAsync());
            Assert.Equal(NexusEventPhase.Cancelled, pendingRelease.Phase);
            var release = new EventServerReservationReleaseRequestV1
            {
                RequestId = Guid.NewGuid(),
                CorrelationId = Guid.NewGuid(),
                EventId = definition.EventId,
                ReservationId = decision.ReservationId!.Value,
                IdempotencyKey = "event-assignment-release",
                ReasonCode = "event_cancelled"
            };
            Assert.True(await restarted.MarkEventServerReservationReleasedAsync(release));
            Assert.True(await restarted.MarkEventServerReservationReleasedAsync(release));
            Assert.True((await restarted.GetEventServerAssignmentAsync(definition.EventId))!.IsReleased);
            Assert.Empty(await restarted.GetTerminalEventsWithUnreleasedServerAssignmentAsync());
        });
    }

    private static NexusEventDefinitionV1 Definition(string eventId)
    {
        var now = DateTimeOffset.UtcNow;
        var opens = new DateTimeOffset(now.Year, now.Month, now.Day, 0, 0, 0, TimeSpan.Zero).AddDays(1);
        return new NexusEventDefinitionV1
        {
            EventId = eventId,
            Revision = 1,
            Phase = NexusEventPhase.Planned,
            RegistrationOpensUtc = opens,
            RegistrationClosesUtc = opens.AddHours(2),
            StartsUtc = opens.AddHours(3),
            EndsUtc = opens.AddHours(5),
            MaximumParticipants = 64,
            TargetSystemId = "li01",
            RulesetId = "arena"
        };
    }

    private static EventLifecycleTransitionRequestV1 Transition(string eventId, string idempotencyKey,
        NexusEventPhase expectedPhase, NexusEventPhase nextPhase, long revision) => new()
        {
            RequestId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            ActorService = "coordinator",
            EventId = eventId,
            ExpectedRevision = revision,
            ExpectedPhase = expectedPhase,
            NextPhase = nextPhase,
            IdempotencyKey = idempotencyKey,
            ReasonCode = nextPhase is NexusEventPhase.Cancelled or NexusEventPhase.Failed ? "operator_cancelled" : null
        };

    private static EventRegistrationRequestV1 Registration(string eventId, Guid accountId, long characterId,
        string idempotencyKey) => new()
        {
            RequestId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            AccountId = accountId,
            SessionId = Guid.NewGuid(),
            CharacterId = characterId,
            LeaseVersion = 1,
            EventId = eventId,
            IdempotencyKey = idempotencyKey
        };

    private static async Task WithDatabaseAsync(Func<string, Task> action)
    {
        var configured = Environment.GetEnvironmentVariable("LANCER_NEXUS_EVENTS_TEST_MYSQL")
            ?? throw new InvalidOperationException("MySQL test server must be configured.");
        var database = $"events_test_{Guid.NewGuid():N}";
        var adminBuilder = new MySqlConnectionStringBuilder(configured) { Database = "information_schema" };
        await using var admin = new MySqlConnection(adminBuilder.ConnectionString);
        await admin.OpenAsync();
        await using var command = admin.CreateCommand();
        command.CommandText = $"CREATE DATABASE `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";
        await command.ExecuteNonQueryAsync();
        var databaseBuilder = new MySqlConnectionStringBuilder(configured) { Database = database };
        try
        {
            await using var connection = new MySqlConnection(databaseBuilder.ConnectionString);
            await connection.OpenAsync();
            var migrationDirectory = Path.Combine(AppContext.BaseDirectory, "db", "migrations");
            foreach (var migrationPath in Directory.GetFiles(migrationDirectory, "*.sql")
                         .Order(StringComparer.Ordinal))
            {
                foreach (var statement in (await File.ReadAllTextAsync(migrationPath))
                             .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                {
                    await using var migration = connection.CreateCommand();
                    migration.CommandText = statement;
                    await migration.ExecuteNonQueryAsync();
                }
            }
            await action(databaseBuilder.ConnectionString);
        }
        finally
        {
            MySqlConnection.ClearAllPools();
            command.CommandText = $"DROP DATABASE IF EXISTS `{database}`";
            await command.ExecuteNonQueryAsync();
        }
    }

    private sealed class MySqlFactAttribute : FactAttribute
    {
        public MySqlFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("LANCER_NEXUS_EVENTS_TEST_MYSQL")))
                Skip = "Set LANCER_NEXUS_EVENTS_TEST_MYSQL to an isolated MySQL test server.";
        }
    }

    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}
