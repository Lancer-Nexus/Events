using System.Security.Cryptography;
using System.Text;
using LancerNexus.Protocol;

namespace LancerNexus.Events;

public sealed class EventServerReservationWorkflow(
    IEventStore store,
    IEventCoordinatorClient coordinator,
    ILogger<EventServerReservationWorkflow> logger)
{
    public async Task<EventServerReservationDecisionV1> EnsureReservedAsync(
        NexusEventDefinitionV1 definition, CancellationToken cancellationToken = default)
    {
        if (definition.Phase != NexusEventPhase.Provisioning)
            return ReservationFailure(definition, EventServerReservationStatus.Conflict, "event_not_provisioning");

        var existing = await store.GetEventServerAssignmentAsync(definition.EventId, cancellationToken);
        if (existing is not null)
        {
            if (existing.IsReleased || existing.Request.EventRevision != definition.Revision)
                return ReservationFailure(definition, EventServerReservationStatus.Conflict,
                    "event_assignment_revision_conflict");
            return existing.Decision with
            {
                Status = EventServerReservationStatus.AlreadyReserved,
                RequestId = ReservationRequestId(definition.EventId, definition.Revision),
                CorrelationId = ReservationCorrelationId(definition.EventId, definition.Revision)
            };
        }

        var request = CreateReservationRequest(definition);
        var decision = await coordinator.ReserveAsync(request, cancellationToken);
        if (!decision.IsValid())
            return ReservationFailure(definition, EventServerReservationStatus.Unavailable,
                "invalid_coordinator_response");
        if (decision.Status is not (EventServerReservationStatus.Reserved or
                EventServerReservationStatus.AlreadyReserved))
            return decision;
        if (!await store.SaveEventServerAssignmentAsync(request, decision, cancellationToken))
        {
            logger.LogError("Coordinator reserved event {EventId}, but its assignment could not be persisted; retry will reconcile the idempotent reservation.", definition.EventId);
            return ReservationFailure(definition, EventServerReservationStatus.Unavailable,
                "assignment_persistence_failed");
        }
        return decision;
    }

    public async Task<bool> EnsureReleasedAsync(EventServerAssignment assignment, NexusEventPhase terminalPhase,
        CancellationToken cancellationToken = default)
    {
        if (terminalPhase is not (NexusEventPhase.Cancelled or NexusEventPhase.Failed or NexusEventPhase.Closed))
            return false;
        if (assignment.IsReleased) return true;

        var reservationId = assignment.Decision.ReservationId;
        if (reservationId is null) return false;
        var idempotencyKey = StableKey("event-rel-", $"{assignment.Request.EventId}|{reservationId:D}");
        var release = new EventServerReservationReleaseRequestV1
        {
            RequestId = StableGuid($"release:{assignment.Request.EventId}:{reservationId:D}"),
            CorrelationId = StableGuid($"release-correlation:{assignment.Request.EventId}:{reservationId:D}"),
            EventId = assignment.Request.EventId,
            ReservationId = reservationId.Value,
            IdempotencyKey = idempotencyKey,
            ReasonCode = "event_" + terminalPhase.ToString().ToLowerInvariant()
        };
        var decision = await coordinator.ReleaseAsync(release, cancellationToken);
        if (!decision.IsValid() || decision.Status is not (EventServerReservationReleaseStatus.Released or
                EventServerReservationReleaseStatus.AlreadyReleased))
            return false;
        if (!await store.MarkEventServerReservationReleasedAsync(release, cancellationToken))
        {
            logger.LogError("Coordinator released event {EventId} reservation {ReservationId}, but the release marker could not be persisted; retry will reconcile it.",
                release.EventId, release.ReservationId);
            return false;
        }
        return true;
    }

    private static EventServerReservationRequestV1 CreateReservationRequest(NexusEventDefinitionV1 definition) => new()
    {
        RequestId = ReservationRequestId(definition.EventId, definition.Revision),
        CorrelationId = ReservationCorrelationId(definition.EventId, definition.Revision),
        EventId = definition.EventId,
        EventRevision = definition.Revision,
        TargetSystemId = definition.TargetSystemId,
        MaximumParticipants = definition.MaximumParticipants,
        StartsUtc = definition.StartsUtc,
        EndsUtc = definition.EndsUtc,
        IdempotencyKey = StableKey("event-res-", $"{definition.EventId}|{definition.Revision}")
    };

    private static Guid ReservationRequestId(string eventId, long revision) =>
        StableGuid($"reservation:{eventId}:{revision}");

    private static Guid ReservationCorrelationId(string eventId, long revision) =>
        StableGuid($"reservation-correlation:{eventId}:{revision}");

    private static string StableKey(string prefix, string value) =>
        prefix + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)))[..40].ToLowerInvariant();

    private static Guid StableGuid(string value) =>
        new(SHA256.HashData(Encoding.UTF8.GetBytes(value))[..16]);

    private static EventServerReservationDecisionV1 ReservationFailure(NexusEventDefinitionV1 definition,
        EventServerReservationStatus status, string errorCode) => new()
        {
            RequestId = ReservationRequestId(definition.EventId, definition.Revision),
            CorrelationId = ReservationCorrelationId(definition.EventId, definition.Revision),
            EventId = definition.EventId,
            Status = status,
            ErrorCode = errorCode
        };
}

public sealed class EventServerReservationRecoveryService(
    IServiceScopeFactory scopeFactory,
    ILogger<EventServerReservationRecoveryService> logger) : BackgroundService
{
    private static readonly TimeSpan RetryInterval = TimeSpan.FromSeconds(10);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RecoverOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception error)
            {
                logger.LogError(error, "Event server reservation recovery pass failed.");
            }

            await Task.Delay(RetryInterval, stoppingToken);
        }
    }

    private async Task RecoverOnceAsync(CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var store = scope.ServiceProvider.GetRequiredService<IEventStore>();
        var workflow = scope.ServiceProvider.GetRequiredService<EventServerReservationWorkflow>();
        foreach (var definition in await store.GetEventsNeedingServerAssignmentAsync(cancellationToken))
        {
            var decision = await workflow.EnsureReservedAsync(definition, cancellationToken);
            if (decision.Status is not (EventServerReservationStatus.Reserved or
                    EventServerReservationStatus.AlreadyReserved))
                logger.LogWarning("Event {EventId} remains in provisioning without a server assignment: {ReasonCode}.",
                    definition.EventId, decision.ErrorCode ?? decision.Status.ToString());
        }

        foreach (var (assignment, phase) in
                 await store.GetTerminalEventsWithUnreleasedServerAssignmentAsync(cancellationToken))
        {
            if (!await workflow.EnsureReleasedAsync(assignment, phase, cancellationToken))
                logger.LogWarning("Event {EventId} has a terminal state but its Coordinator capacity release is pending.",
                    assignment.Request.EventId);
        }
    }
}
