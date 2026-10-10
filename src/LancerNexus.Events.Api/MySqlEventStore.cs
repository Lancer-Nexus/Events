using System.Data;
using System.Security.Cryptography;
using System.Text.Json;
using LancerNexus.Protocol;
using MySqlConnector;

namespace LancerNexus.Events;

public sealed class MySqlEventStore(string? connectionString, TimeProvider timeProvider) : IEventStore
{
    private static readonly string[] RequiredTables =
    [
        "nexus_events", "event_definition_audit", "event_lifecycle_requests", "event_lifecycle_audit",
        "event_registrations", "event_registration_requests", "event_registration_audit",
        "event_server_assignments"
    ];

    public async Task<bool> IsReadyAsync(CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;
        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            foreach (var table in RequiredTables)
            {
                await using var command = connection.CreateCommand();
                command.CommandText = """
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = DATABASE() AND table_name = @table_name;
                    """;
                command.Parameters.AddWithValue("@table_name", table);
                if (Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken)) != 1)
                    return false;
            }
            return true;
        }
        catch (MySqlException)
        {
            return false;
        }
    }

    public async Task<NexusEventDefinitionV1?> GetEventAsync(string eventId,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException("Events MySQL is not configured.");
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = SelectEventSql + " WHERE event_id = @event_id LIMIT 1;";
        command.Parameters.AddWithValue("@event_id", eventId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadEvent(reader) : null;
    }

    public async Task<EventDefinitionWriteResult> CreateEventAsync(NexusEventDefinitionV1 definition,
        string actorService, CancellationToken cancellationToken = default)
    {
        if (!definition.IsValid() || definition.Revision != 1 || definition.Phase != NexusEventPhase.Planned ||
            !AdminQuery.ValidIdentifier(actorService))
            return new(EventDefinitionWriteStatus.Invalid, null, "invalid_event_definition");
        if (string.IsNullOrWhiteSpace(connectionString))
            return new(EventDefinitionWriteStatus.Unavailable, null, "storage_unavailable");

        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            var now = timeProvider.GetUtcNow().UtcDateTime;
            await using (var insert = connection.CreateCommand())
            {
                insert.Transaction = transaction;
                insert.CommandText = """
                    INSERT INTO nexus_events (
                        event_id, revision, phase, registration_opens_utc, registration_closes_utc,
                        starts_utc, ends_utc, maximum_participants, target_system_id, ruleset_id,
                        created_at_utc, updated_at_utc)
                    VALUES (@event_id, 1, @phase, @registration_opens, @registration_closes,
                        @starts, @ends, @maximum_participants, @target_system_id, @ruleset_id, @now, @now);
                    """;
                AddDefinitionParameters(insert, definition);
                insert.Parameters.AddWithValue("@now", now);
                await insert.ExecuteNonQueryAsync(cancellationToken);
            }
            await using (var audit = connection.CreateCommand())
            {
                audit.Transaction = transaction;
                audit.CommandText = """
                    INSERT INTO event_definition_audit (event_id, actor_service, definition_revision, created_at_utc)
                    VALUES (@event_id, @actor_service, 1, @now);
                    """;
                audit.Parameters.AddWithValue("@event_id", definition.EventId);
                audit.Parameters.AddWithValue("@actor_service", actorService);
                audit.Parameters.AddWithValue("@now", now);
                await audit.ExecuteNonQueryAsync(cancellationToken);
            }
            await transaction.CommitAsync(cancellationToken);
            return new(EventDefinitionWriteStatus.Created, definition, null);
        }
        catch (MySqlException error) when (error.Number == 1062)
        {
            var existing = await GetEventAsync(definition.EventId, cancellationToken);
            return new(existing == definition ? EventDefinitionWriteStatus.AlreadyExists : EventDefinitionWriteStatus.Conflict,
                existing, existing == definition ? null : "event_id_conflict");
        }
        catch (MySqlException)
        {
            return new(EventDefinitionWriteStatus.Unavailable, null, "storage_unavailable");
        }
    }

    public async Task<EventRegistrationDecisionV1> RegisterAsync(EventRegistrationRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        if (!request.IsValid())
            return RegistrationDecision(request, EventRegistrationStatus.Invalid, "invalid_registration_request");
        if (string.IsNullOrWhiteSpace(connectionString))
            return RegistrationDecision(request, EventRegistrationStatus.Unavailable, "storage_unavailable");

        var fingerprint = SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(request));
        var accountId = request.AccountId.ToString("D");
        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(
                IsolationLevel.ReadCommitted, cancellationToken);

            var replay = await FindRegistrationRequestAsync(connection, transaction, request.RequestId,
                cancellationToken);
            if (replay is not null)
            {
                if (CryptographicOperations.FixedTimeEquals(replay.Value.Hash, fingerprint))
                    return JsonSerializer.Deserialize<EventRegistrationDecisionV1>(replay.Value.Json)!;
                return RegistrationDecision(request, EventRegistrationStatus.Conflict, "request_id_reused");
            }

            if (await RegistrationIdempotencyKeyExistsAsync(connection, transaction, accountId,
                    request.IdempotencyKey, cancellationToken))
            {
                var collision = RegistrationDecision(request, EventRegistrationStatus.Conflict,
                    "idempotency_key_reused");
                await WriteRegistrationAuditAsync(connection, transaction, request, collision, fingerprint,
                    cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return collision;
            }

            var current = await ReadEventForUpdateAsync(connection, transaction, request.EventId, cancellationToken);
            // Recheck after taking the event lock: an identical concurrent registration
            // may have committed while this request waited for the lock.
            replay = await FindRegistrationRequestAsync(connection, transaction, request.RequestId,
                cancellationToken);
            if (replay is not null)
            {
                if (CryptographicOperations.FixedTimeEquals(replay.Value.Hash, fingerprint))
                    return JsonSerializer.Deserialize<EventRegistrationDecisionV1>(replay.Value.Json)!;
                return RegistrationDecision(request, EventRegistrationStatus.Conflict, "request_id_reused");
            }
            if (await RegistrationIdempotencyKeyExistsAsync(connection, transaction, accountId,
                    request.IdempotencyKey, cancellationToken))
            {
                var collision = RegistrationDecision(request, EventRegistrationStatus.Conflict,
                    "idempotency_key_reused");
                await WriteRegistrationAuditAsync(connection, transaction, request, collision, fingerprint,
                    cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return collision;
            }

            EventRegistrationDecisionV1 decision;
            var registrationId = current is null
                ? null
                : await FindExistingRegistrationAsync(connection, transaction, request, cancellationToken);
            if (current is null)
            {
                decision = RegistrationDecision(request, EventRegistrationStatus.EventNotFound, "event_not_found");
            }
            else if (registrationId is { } existing && existing.AccountId == request.AccountId)
            {
                decision = RegistrationDecision(request, EventRegistrationStatus.AlreadyRegistered, null,
                    existing.RegistrationId, current.Revision);
            }
            else if (registrationId is not null)
            {
                decision = RegistrationDecision(request, EventRegistrationStatus.Denied,
                    "character_already_registered");
            }
            else
            {
                var now = timeProvider.GetUtcNow();
                if (current.Phase != NexusEventPhase.Registration || now < current.RegistrationOpensUtc ||
                    now >= current.RegistrationClosesUtc)
                {
                    decision = RegistrationDecision(request, EventRegistrationStatus.RegistrationClosed,
                        now < current.RegistrationOpensUtc ? "registration_not_open" : "registration_closed");
                }
                else if (await CountRegistrationsAsync(connection, transaction, request.EventId,
                             cancellationToken) >= current.MaximumParticipants)
                {
                    decision = RegistrationDecision(request, EventRegistrationStatus.CapacityFull,
                        "capacity_full");
                }
                else
                {
                    var id = Guid.NewGuid();
                    await InsertRegistrationAsync(connection, transaction, request, id, now.UtcDateTime,
                        cancellationToken);
                    decision = RegistrationDecision(request, EventRegistrationStatus.Accepted, null, id,
                        current.Revision);
                }
            }

            await SaveRegistrationDecisionAsync(connection, transaction, request, decision, fingerprint,
                cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return decision;
        }
        catch (MySqlException error) when (error.Number == 1062)
        {
            return RegistrationDecision(request, EventRegistrationStatus.Conflict, "idempotency_key_reused");
        }
        catch (MySqlException)
        {
            return RegistrationDecision(request, EventRegistrationStatus.Unavailable, "storage_unavailable");
        }
    }

    public async Task<EventLifecycleTransitionDecisionV1> TransitionAsync(
        EventLifecycleTransitionRequestV1 request, string actorId, CancellationToken cancellationToken = default)
    {
        if (!request.IsValid() || actorId is not { Length: > 0 and <= 128 })
            return Decision(request, EventLifecycleStatus.Invalid, "invalid_request");
        if (string.IsNullOrWhiteSpace(connectionString))
            return Decision(request, EventLifecycleStatus.Unavailable, "storage_unavailable");

        var fingerprint = SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(request));
        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            // READ COMMITTED avoids next-key gap-lock deadlocks on the missing
            // request/idempotency rows. The event row itself remains locked below.
            await using var transaction = await connection.BeginTransactionAsync(
                IsolationLevel.ReadCommitted, cancellationToken);

            var replay = await FindRequestAsync(connection, transaction, request.RequestId, cancellationToken);
            if (replay is not null)
            {
                if (CryptographicOperations.FixedTimeEquals(replay.Value.Hash, fingerprint))
                    return JsonSerializer.Deserialize<EventLifecycleTransitionDecisionV1>(replay.Value.Json)!;
                return Decision(request, EventLifecycleStatus.Conflict, "request_id_reused");
            }

            if (await IdempotencyKeyExistsAsync(connection, transaction, actorId,
                    request.IdempotencyKey, cancellationToken))
            {
                var collision = Decision(request, EventLifecycleStatus.Conflict, "idempotency_key_reused");
                await WriteAuditAsync(connection, transaction, request, actorId, collision, fingerprint,
                    previousPhase: null, previousRevision: null, cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return collision;
            }

            var current = await ReadEventForUpdateAsync(connection, transaction, request.EventId, cancellationToken);
            EventLifecycleTransitionDecisionV1 decision;
            NexusEventPhase? previousPhase = current?.Phase;
            long? previousRevision = current?.Revision;
            if (current is null)
            {
                decision = Decision(request, EventLifecycleStatus.EventNotFound, "event_not_found");
            }
            else if (current.Revision != request.ExpectedRevision || current.Phase != request.ExpectedPhase)
            {
                decision = Decision(request, EventLifecycleStatus.Conflict, "event_revision_conflict",
                    current.Phase, current.Revision);
            }
            else
            {
                var nextRevision = checked(current.Revision + 1);
                await using var update = connection.CreateCommand();
                update.Transaction = transaction;
                update.CommandText = """
                    UPDATE nexus_events SET phase = @next_phase, revision = @next_revision,
                        updated_at_utc = @updated_at
                    WHERE event_id = @event_id AND revision = @expected_revision AND phase = @expected_phase;
                    """;
                update.Parameters.AddWithValue("@next_phase", (byte)request.NextPhase);
                update.Parameters.AddWithValue("@next_revision", nextRevision);
                update.Parameters.AddWithValue("@updated_at", timeProvider.GetUtcNow().UtcDateTime);
                update.Parameters.AddWithValue("@event_id", request.EventId);
                update.Parameters.AddWithValue("@expected_revision", request.ExpectedRevision);
                update.Parameters.AddWithValue("@expected_phase", (byte)request.ExpectedPhase);
                if (await update.ExecuteNonQueryAsync(cancellationToken) != 1)
                    throw new InvalidOperationException("Locked event changed during lifecycle transition.");
                decision = Decision(request, EventLifecycleStatus.Applied, null, request.NextPhase, nextRevision);
            }

            await SaveDecisionAsync(connection, transaction, request, actorId, decision, fingerprint,
                previousPhase, previousRevision, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return decision;
        }
        catch (MySqlException error) when (error.Number == 1062)
        {
            return Decision(request, EventLifecycleStatus.Conflict, "idempotency_key_reused");
        }
        catch (MySqlException)
        {
            return Decision(request, EventLifecycleStatus.Unavailable, "storage_unavailable");
        }
    }

    public async Task<EventServerAssignment?> GetEventServerAssignmentAsync(string eventId,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException("Events MySQL is not configured.");
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT reservation_request_json, reservation_decision_json, released_at_utc,
                   release_idempotency_key
            FROM event_server_assignments WHERE event_id = @event_id LIMIT 1;
            """;
        command.Parameters.AddWithValue("@event_id", eventId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        var request = JsonSerializer.Deserialize<EventServerReservationRequestV1>(reader.GetString(0))
                      ?? throw new InvalidDataException("Stored event reservation request is invalid.");
        var decision = JsonSerializer.Deserialize<EventServerReservationDecisionV1>(reader.GetString(1))
                       ?? throw new InvalidDataException("Stored event reservation decision is invalid.");
        return new EventServerAssignment(request, decision, !reader.IsDBNull(2),
            reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    public async Task<bool> SaveEventServerAssignmentAsync(EventServerReservationRequestV1 request,
        EventServerReservationDecisionV1 decision, CancellationToken cancellationToken = default)
    {
        if (!request.IsValid() || !decision.IsValid() ||
            decision.Status is not (EventServerReservationStatus.Reserved or EventServerReservationStatus.AlreadyReserved) ||
            decision.EventId != request.EventId || decision.ReservedCapacity != request.MaximumParticipants ||
            decision.SystemId is null || !string.Equals(decision.SystemId, request.TargetSystemId,
                StringComparison.OrdinalIgnoreCase))
            return false;
        if (string.IsNullOrWhiteSpace(connectionString)) return false;

        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = """
                INSERT INTO event_server_assignments (event_id, event_revision, reservation_id,
                    instance_id, target_system_id, endpoint, reserved_capacity, reservation_request_json,
                    reservation_decision_json, created_at_utc, updated_at_utc)
                VALUES (@event_id, @event_revision, @reservation_id, @instance_id, @target_system_id,
                    @endpoint, @reserved_capacity, @request, @decision, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6));
                """;
            command.Parameters.AddWithValue("@event_id", request.EventId);
            command.Parameters.AddWithValue("@event_revision", request.EventRevision);
            command.Parameters.AddWithValue("@reservation_id", decision.ReservationId!.Value.ToString("D"));
            command.Parameters.AddWithValue("@instance_id", decision.InstanceId!);
            command.Parameters.AddWithValue("@target_system_id", decision.SystemId!);
            command.Parameters.AddWithValue("@endpoint", decision.Endpoint!);
            command.Parameters.AddWithValue("@reserved_capacity", decision.ReservedCapacity!.Value);
            command.Parameters.AddWithValue("@request", JsonSerializer.Serialize(request));
            command.Parameters.AddWithValue("@decision", JsonSerializer.Serialize(decision));
            await command.ExecuteNonQueryAsync(cancellationToken);
            return true;
        }
        catch (MySqlException error) when (error.Number == 1062)
        {
            var existing = await GetEventServerAssignmentAsync(request.EventId, cancellationToken);
            return existing is not null && SameReservation(existing, request, decision);
        }
        catch (MySqlException)
        {
            return false;
        }
    }

    public async Task<bool> MarkEventServerReservationReleasedAsync(
        EventServerReservationReleaseRequestV1 request, CancellationToken cancellationToken = default)
    {
        if (!request.IsValid() || string.IsNullOrWhiteSpace(connectionString)) return false;
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var read = connection.CreateCommand();
        read.Transaction = transaction;
        read.CommandText = """
            SELECT reservation_id, release_idempotency_key FROM event_server_assignments
            WHERE event_id = @event_id FOR UPDATE;
            """;
        read.Parameters.AddWithValue("@event_id", request.EventId);
        await using var reader = await read.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return false;
        var reservationId = reader.GetGuid(0);
        var oldKey = reader.IsDBNull(1) ? null : reader.GetString(1);
        await reader.CloseAsync();
        if (reservationId != request.ReservationId) return false;
        if (oldKey is not null)
        {
            if (!string.Equals(oldKey, request.IdempotencyKey, StringComparison.Ordinal)) return false;
            await transaction.CommitAsync(cancellationToken);
            return true;
        }
        await using var update = connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText = """
            UPDATE event_server_assignments SET released_at_utc = UTC_TIMESTAMP(6),
                release_idempotency_key = @release_key, updated_at_utc = UTC_TIMESTAMP(6)
            WHERE event_id = @event_id AND reservation_id = @reservation_id AND released_at_utc IS NULL;
            """;
        update.Parameters.AddWithValue("@release_key", request.IdempotencyKey);
        update.Parameters.AddWithValue("@event_id", request.EventId);
        update.Parameters.AddWithValue("@reservation_id", request.ReservationId.ToString("D"));
        if (await update.ExecuteNonQueryAsync(cancellationToken) != 1) return false;
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    public async Task<IReadOnlyList<NexusEventDefinitionV1>> GetEventsNeedingServerAssignmentAsync(
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return [];
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT e.event_id, e.revision, e.phase, e.registration_opens_utc, e.registration_closes_utc,
                   e.starts_utc, e.ends_utc, e.maximum_participants, e.target_system_id, e.ruleset_id
            FROM nexus_events e LEFT JOIN event_server_assignments a ON a.event_id = e.event_id
            WHERE e.phase = @phase AND a.event_id IS NULL ORDER BY e.starts_utc LIMIT 100;
            """;
        command.Parameters.AddWithValue("@phase", (byte)NexusEventPhase.Provisioning);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var events = new List<NexusEventDefinitionV1>();
        while (await reader.ReadAsync(cancellationToken)) events.Add(ReadEvent(reader));
        return events;
    }

    public async Task<IReadOnlyList<(EventServerAssignment Assignment, NexusEventPhase Phase)>>
        GetTerminalEventsWithUnreleasedServerAssignmentAsync(CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return [];
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT a.reservation_request_json, a.reservation_decision_json, a.released_at_utc,
                   a.release_idempotency_key, e.phase
            FROM event_server_assignments a JOIN nexus_events e ON e.event_id = a.event_id
            WHERE e.phase IN (@cancelled, @failed, @closed) AND a.released_at_utc IS NULL
            ORDER BY a.created_at_utc LIMIT 100;
            """;
        command.Parameters.AddWithValue("@cancelled", (byte)NexusEventPhase.Cancelled);
        command.Parameters.AddWithValue("@failed", (byte)NexusEventPhase.Failed);
        command.Parameters.AddWithValue("@closed", (byte)NexusEventPhase.Closed);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var assignments = new List<(EventServerAssignment Assignment, NexusEventPhase Phase)>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var request = JsonSerializer.Deserialize<EventServerReservationRequestV1>(reader.GetString(0))
                          ?? throw new InvalidDataException("Stored event reservation request is invalid.");
            var decision = JsonSerializer.Deserialize<EventServerReservationDecisionV1>(reader.GetString(1))
                           ?? throw new InvalidDataException("Stored event reservation decision is invalid.");
            assignments.Add((new EventServerAssignment(request, decision, !reader.IsDBNull(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)), (NexusEventPhase)reader.GetByte(4)));
        }
        return assignments;
    }

    private static bool SameReservation(EventServerAssignment existing,
        EventServerReservationRequestV1 request, EventServerReservationDecisionV1 decision) =>
        existing.Request == request && existing.Decision.ReservationId == decision.ReservationId &&
        existing.Decision.InstanceId == decision.InstanceId && existing.Decision.SystemId == decision.SystemId &&
        existing.Decision.Endpoint == decision.Endpoint && existing.Decision.ReservedCapacity == decision.ReservedCapacity;

    private static async Task<(string Json, byte[] Hash)?> FindRequestAsync(MySqlConnection connection,
        MySqlTransaction transaction, Guid requestId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT decision_json, payload_sha256 FROM event_lifecycle_requests WHERE request_id = @request_id FOR UPDATE;";
        command.Parameters.AddWithValue("@request_id", requestId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? (reader.GetString(0), reader.GetFieldValue<byte[]>(1)) : null;
    }

    private static async Task<(string Json, byte[] Hash)?> FindRegistrationRequestAsync(
        MySqlConnection connection, MySqlTransaction transaction, Guid requestId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT decision_json, payload_sha256 FROM event_registration_requests WHERE request_id = @request_id FOR UPDATE;";
        command.Parameters.AddWithValue("@request_id", requestId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? (reader.GetString(0), reader.GetFieldValue<byte[]>(1)) : null;
    }

    private static async Task<bool> RegistrationIdempotencyKeyExistsAsync(MySqlConnection connection,
        MySqlTransaction transaction, string accountId, string idempotencyKey,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT 1 FROM event_registration_requests WHERE account_id = @account_id AND idempotency_key = @idempotency_key LIMIT 1 FOR UPDATE;";
        command.Parameters.AddWithValue("@account_id", accountId);
        command.Parameters.AddWithValue("@idempotency_key", idempotencyKey);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    private static async Task<ExistingRegistration?> FindExistingRegistrationAsync(MySqlConnection connection,
        MySqlTransaction transaction, EventRegistrationRequestV1 request,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT registration_id, account_id FROM event_registrations
            WHERE event_id = @event_id AND (account_id = @account_id OR character_id = @character_id)
            LIMIT 1 FOR UPDATE;
            """;
        command.Parameters.AddWithValue("@event_id", request.EventId);
        command.Parameters.AddWithValue("@account_id", request.AccountId.ToString("D"));
        command.Parameters.AddWithValue("@character_id", request.CharacterId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        return new ExistingRegistration(reader.GetGuid(0), reader.GetGuid(1));
    }

    private static async Task<long> CountRegistrationsAsync(MySqlConnection connection,
        MySqlTransaction transaction, string eventId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT COUNT(*) FROM event_registrations WHERE event_id = @event_id;";
        command.Parameters.AddWithValue("@event_id", eventId);
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken));
    }

    private static async Task InsertRegistrationAsync(MySqlConnection connection, MySqlTransaction transaction,
        EventRegistrationRequestV1 request, Guid registrationId, DateTime registeredAtUtc,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO event_registrations (registration_id, event_id, account_id, session_id,
                character_id, lease_version, registered_at_utc)
            VALUES (@registration_id, @event_id, @account_id, @session_id, @character_id,
                @lease_version, @registered_at);
            """;
        command.Parameters.AddWithValue("@registration_id", registrationId.ToString("D"));
        command.Parameters.AddWithValue("@event_id", request.EventId);
        command.Parameters.AddWithValue("@account_id", request.AccountId.ToString("D"));
        command.Parameters.AddWithValue("@session_id", request.SessionId.ToString("D"));
        command.Parameters.AddWithValue("@character_id", request.CharacterId);
        command.Parameters.AddWithValue("@lease_version", request.LeaseVersion);
        command.Parameters.AddWithValue("@registered_at", registeredAtUtc);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task SaveRegistrationDecisionAsync(MySqlConnection connection,
        MySqlTransaction transaction, EventRegistrationRequestV1 request,
        EventRegistrationDecisionV1 decision, byte[] fingerprint, CancellationToken cancellationToken)
    {
        await using (var save = connection.CreateCommand())
        {
            save.Transaction = transaction;
            save.CommandText = """
                INSERT INTO event_registration_requests (request_id, account_id, idempotency_key,
                    payload_sha256, decision_json, created_at_utc)
                VALUES (@request_id, @account_id, @idempotency_key, @fingerprint, @decision, UTC_TIMESTAMP(6));
                """;
            save.Parameters.AddWithValue("@request_id", request.RequestId.ToString("D"));
            save.Parameters.AddWithValue("@account_id", request.AccountId.ToString("D"));
            save.Parameters.AddWithValue("@idempotency_key", request.IdempotencyKey);
            save.Parameters.AddWithValue("@fingerprint", fingerprint);
            save.Parameters.AddWithValue("@decision", JsonSerializer.Serialize(decision));
            await save.ExecuteNonQueryAsync(cancellationToken);
        }
        await WriteRegistrationAuditAsync(connection, transaction, request, decision, fingerprint,
            cancellationToken);
    }

    private static async Task WriteRegistrationAuditAsync(MySqlConnection connection,
        MySqlTransaction transaction, EventRegistrationRequestV1 request,
        EventRegistrationDecisionV1 decision, byte[] fingerprint, CancellationToken cancellationToken)
    {
        await using var audit = connection.CreateCommand();
        audit.Transaction = transaction;
        audit.CommandText = """
            INSERT INTO event_registration_audit (request_id, correlation_id, event_id, account_id,
                status, registration_id, error_code, payload_sha256, created_at_utc)
            VALUES (@request_id, @correlation_id, @event_id, @account_id, @status, @registration_id,
                @error_code, @fingerprint, UTC_TIMESTAMP(6));
            """;
        audit.Parameters.AddWithValue("@request_id", request.RequestId.ToString("D"));
        audit.Parameters.AddWithValue("@correlation_id", request.CorrelationId.ToString("D"));
        audit.Parameters.AddWithValue("@event_id", request.EventId);
        audit.Parameters.AddWithValue("@account_id", request.AccountId.ToString("D"));
        audit.Parameters.AddWithValue("@status", (byte)decision.Status);
        audit.Parameters.AddWithValue("@registration_id", decision.RegistrationId is { } id
            ? id.ToString("D") : DBNull.Value);
        audit.Parameters.AddWithValue("@error_code", decision.ErrorCode is { } errorCode
            ? errorCode : DBNull.Value);
        audit.Parameters.AddWithValue("@fingerprint", fingerprint);
        await audit.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<bool> IdempotencyKeyExistsAsync(MySqlConnection connection,
        MySqlTransaction transaction, string actorId, string idempotencyKey, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT 1 FROM event_lifecycle_requests WHERE actor_id = @actor_id AND idempotency_key = @idempotency_key LIMIT 1 FOR UPDATE;";
        command.Parameters.AddWithValue("@actor_id", actorId);
        command.Parameters.AddWithValue("@idempotency_key", idempotencyKey);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    private static async Task<NexusEventDefinitionV1?> ReadEventForUpdateAsync(MySqlConnection connection,
        MySqlTransaction transaction, string eventId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = SelectEventSql + " WHERE event_id = @event_id LIMIT 1 FOR UPDATE;";
        command.Parameters.AddWithValue("@event_id", eventId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadEvent(reader) : null;
    }

    private static async Task SaveDecisionAsync(MySqlConnection connection, MySqlTransaction transaction,
        EventLifecycleTransitionRequestV1 request, string actorId, EventLifecycleTransitionDecisionV1 decision,
        byte[] fingerprint, NexusEventPhase? previousPhase, long? previousRevision,
        CancellationToken cancellationToken)
    {
        await using (var save = connection.CreateCommand())
        {
            save.Transaction = transaction;
            save.CommandText = """
                INSERT INTO event_lifecycle_requests (request_id, actor_id, idempotency_key, payload_sha256,
                    decision_json, created_at_utc)
                VALUES (@request_id, @actor_id, @idempotency_key, @fingerprint, @decision, UTC_TIMESTAMP(6));
                """;
            save.Parameters.AddWithValue("@request_id", request.RequestId.ToString("D"));
            save.Parameters.AddWithValue("@actor_id", actorId);
            save.Parameters.AddWithValue("@idempotency_key", request.IdempotencyKey);
            save.Parameters.AddWithValue("@fingerprint", fingerprint);
            save.Parameters.AddWithValue("@decision", JsonSerializer.Serialize(decision));
            await save.ExecuteNonQueryAsync(cancellationToken);
        }
        await WriteAuditAsync(connection, transaction, request, actorId, decision, fingerprint,
            previousPhase, previousRevision, cancellationToken);
    }

    private static async Task WriteAuditAsync(MySqlConnection connection, MySqlTransaction transaction,
        EventLifecycleTransitionRequestV1 request, string actorId, EventLifecycleTransitionDecisionV1 decision,
        byte[] fingerprint, NexusEventPhase? previousPhase, long? previousRevision,
        CancellationToken cancellationToken)
    {
        await using var audit = connection.CreateCommand();
        audit.Transaction = transaction;
        audit.CommandText = """
            INSERT INTO event_lifecycle_audit (request_id, correlation_id, event_id, actor_id, status,
                previous_phase, next_phase, previous_revision, next_revision, error_code, payload_sha256, created_at_utc)
            VALUES (@request_id, @correlation_id, @event_id, @actor_id, @status, @previous_phase,
                @next_phase, @previous_revision, @next_revision, @error_code, @fingerprint, UTC_TIMESTAMP(6));
            """;
        audit.Parameters.AddWithValue("@request_id", request.RequestId.ToString("D"));
        audit.Parameters.AddWithValue("@correlation_id", request.CorrelationId.ToString("D"));
        audit.Parameters.AddWithValue("@event_id", request.EventId);
        audit.Parameters.AddWithValue("@actor_id", actorId);
        audit.Parameters.AddWithValue("@status", (byte)decision.Status);
        audit.Parameters.AddWithValue("@previous_phase", previousPhase is null ? DBNull.Value : (byte)previousPhase.Value);
        audit.Parameters.AddWithValue("@next_phase", decision.CurrentPhase is null ? DBNull.Value : (byte)decision.CurrentPhase.Value);
        audit.Parameters.AddWithValue("@previous_revision", previousRevision is null ? DBNull.Value : previousRevision.Value);
        audit.Parameters.AddWithValue("@next_revision", decision.CurrentRevision is null ? DBNull.Value : decision.CurrentRevision.Value);
        audit.Parameters.AddWithValue("@error_code", decision.ErrorCode is null ? DBNull.Value : decision.ErrorCode);
        audit.Parameters.AddWithValue("@fingerprint", fingerprint);
        await audit.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddDefinitionParameters(MySqlCommand command, NexusEventDefinitionV1 definition)
    {
        command.Parameters.AddWithValue("@event_id", definition.EventId);
        command.Parameters.AddWithValue("@phase", (byte)definition.Phase);
        command.Parameters.AddWithValue("@registration_opens", definition.RegistrationOpensUtc.UtcDateTime);
        command.Parameters.AddWithValue("@registration_closes", definition.RegistrationClosesUtc.UtcDateTime);
        command.Parameters.AddWithValue("@starts", definition.StartsUtc.UtcDateTime);
        command.Parameters.AddWithValue("@ends", definition.EndsUtc.UtcDateTime);
        command.Parameters.AddWithValue("@maximum_participants", definition.MaximumParticipants);
        command.Parameters.AddWithValue("@target_system_id", definition.TargetSystemId);
        command.Parameters.AddWithValue("@ruleset_id", definition.RulesetId);
    }

    private static NexusEventDefinitionV1 ReadEvent(MySqlDataReader reader) => new()
    {
        EventId = reader.GetString(0),
        Revision = reader.GetInt64(1),
        Phase = (NexusEventPhase)reader.GetByte(2),
        RegistrationOpensUtc = new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc)),
        RegistrationClosesUtc = new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc)),
        StartsUtc = new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc)),
        EndsUtc = new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(6), DateTimeKind.Utc)),
        MaximumParticipants = reader.GetInt32(7),
        TargetSystemId = reader.GetString(8),
        RulesetId = reader.GetString(9)
    };

    private static EventLifecycleTransitionDecisionV1 Decision(EventLifecycleTransitionRequestV1 request,
        EventLifecycleStatus status, string? errorCode, NexusEventPhase? currentPhase = null,
        long? currentRevision = null) => new()
        {
            RequestId = request.RequestId,
            CorrelationId = request.CorrelationId,
            Status = status,
            CurrentPhase = currentPhase,
            CurrentRevision = currentRevision,
            ErrorCode = errorCode
        };

    private static EventRegistrationDecisionV1 RegistrationDecision(EventRegistrationRequestV1 request,
        EventRegistrationStatus status, string? errorCode, Guid? registrationId = null,
        long? eventRevision = null) => new()
        {
            RequestId = request.RequestId,
            CorrelationId = request.CorrelationId,
            Status = status,
            RegistrationId = registrationId,
            EventRevision = eventRevision,
            ErrorCode = errorCode
        };

    private sealed record ExistingRegistration(Guid RegistrationId, Guid AccountId);

    private const string SelectEventSql = """
        SELECT event_id, revision, phase, registration_opens_utc, registration_closes_utc,
               starts_utc, ends_utc, maximum_participants, target_system_id, ruleset_id
        FROM nexus_events
        """;
}
