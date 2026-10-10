using LancerNexus.Events;
using LancerNexus.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace LancerNexus.Events.Tests;

public sealed class EventServerReservationWorkflowTests
{
    [Fact]
    public async Task EnsureReservedAsync_ReplaysSameCoordinatorRequestUntilAssignmentIsDurable()
    {
        var definition = Definition();
        var store = new MemoryEventStore(definition) { FailNextAssignmentSave = true };
        var coordinator = new MemoryCoordinatorClient();
        var workflow = new EventServerReservationWorkflow(store, coordinator,
            NullLogger<EventServerReservationWorkflow>.Instance);

        var first = await workflow.EnsureReservedAsync(definition);
        var second = await workflow.EnsureReservedAsync(definition);
        var third = await workflow.EnsureReservedAsync(definition);

        Assert.Equal(EventServerReservationStatus.Unavailable, first.Status);
        Assert.Equal("assignment_persistence_failed", first.ErrorCode);
        Assert.Equal(EventServerReservationStatus.AlreadyReserved, second.Status);
        Assert.Equal(EventServerReservationStatus.AlreadyReserved, third.Status);
        Assert.Equal(2, coordinator.ReserveRequests.Count);
        Assert.Equal(coordinator.ReserveRequests[0], coordinator.ReserveRequests[1]);
        Assert.Equal(coordinator.ReserveRequests[0].RequestId,
            (await store.GetEventServerAssignmentAsync(definition.EventId))!.Request.RequestId);
        Assert.Equal(2, coordinator.ReserveRequests[0].EventRevision);
    }

    [Fact]
    public async Task EnsureReleasedAsync_RetriesCoordinatorReleaseAndPersistsTheTombstone()
    {
        var definition = Definition();
        var store = new MemoryEventStore(definition);
        var coordinator = new MemoryCoordinatorClient { ReleaseUnavailableResponses = 1 };
        var workflow = new EventServerReservationWorkflow(store, coordinator,
            NullLogger<EventServerReservationWorkflow>.Instance);
        var reservationRequest = new EventServerReservationRequestV1
        {
            RequestId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            EventId = definition.EventId,
            EventRevision = definition.Revision,
            TargetSystemId = definition.TargetSystemId,
            MaximumParticipants = definition.MaximumParticipants,
            StartsUtc = definition.StartsUtc,
            EndsUtc = definition.EndsUtc,
            IdempotencyKey = "reservation-key"
        };
        var reservationDecision = coordinator.Decision(reservationRequest,
            EventServerReservationStatus.Reserved);
        Assert.True(await store.SaveEventServerAssignmentAsync(reservationRequest, reservationDecision));
        var assignment = (await store.GetEventServerAssignmentAsync(definition.EventId))!;

        Assert.False(await workflow.EnsureReleasedAsync(assignment, NexusEventPhase.Closed));
        Assert.True(await workflow.EnsureReleasedAsync(assignment, NexusEventPhase.Closed));

        Assert.Equal(2, coordinator.ReleaseRequests.Count);
        Assert.Equal(coordinator.ReleaseRequests[0], coordinator.ReleaseRequests[1]);
        Assert.True((await store.GetEventServerAssignmentAsync(definition.EventId))!.IsReleased);
    }

    private static NexusEventDefinitionV1 Definition() => new()
    {
        EventId = "event-workflow-test",
        Revision = 2,
        Phase = NexusEventPhase.Provisioning,
        RegistrationOpensUtc = DateTimeOffset.Parse("2026-10-10T10:00:00Z"),
        RegistrationClosesUtc = DateTimeOffset.Parse("2026-10-10T11:00:00Z"),
        StartsUtc = DateTimeOffset.Parse("2026-10-10T12:00:00Z"),
        EndsUtc = DateTimeOffset.Parse("2026-10-10T14:00:00Z"),
        MaximumParticipants = 12,
        TargetSystemId = "li01",
        RulesetId = "duel"
    };

    private sealed class MemoryCoordinatorClient : IEventCoordinatorClient
    {
        public List<EventServerReservationRequestV1> ReserveRequests { get; } = [];
        public List<EventServerReservationReleaseRequestV1> ReleaseRequests { get; } = [];
        public int ReleaseUnavailableResponses { get; init; }
        private readonly Guid reservationId = Guid.NewGuid();

        public Task<EventServerReservationDecisionV1> ReserveAsync(EventServerReservationRequestV1 request,
            CancellationToken cancellationToken = default)
        {
            ReserveRequests.Add(request);
            var status = ReserveRequests.Count == 1
                ? EventServerReservationStatus.Reserved
                : EventServerReservationStatus.AlreadyReserved;
            return Task.FromResult(Decision(request, status));
        }

        public Task<EventServerReservationReleaseDecisionV1> ReleaseAsync(
            EventServerReservationReleaseRequestV1 request, CancellationToken cancellationToken = default)
        {
            ReleaseRequests.Add(request);
            var status = ReleaseRequests.Count <= ReleaseUnavailableResponses
                ? EventServerReservationReleaseStatus.Unavailable
                : ReleaseRequests.Count == ReleaseUnavailableResponses + 1
                    ? EventServerReservationReleaseStatus.Released
                    : EventServerReservationReleaseStatus.AlreadyReleased;
            return Task.FromResult(new EventServerReservationReleaseDecisionV1
            {
                RequestId = request.RequestId,
                CorrelationId = request.CorrelationId,
                EventId = request.EventId,
                ReservationId = status is EventServerReservationReleaseStatus.Released or
                    EventServerReservationReleaseStatus.AlreadyReleased ? request.ReservationId : null,
                Status = status,
                ErrorCode = status == EventServerReservationReleaseStatus.Unavailable
                    ? "coordinator_unavailable" : null
            });
        }

        public EventServerReservationDecisionV1 Decision(EventServerReservationRequestV1 request,
            EventServerReservationStatus status) => new()
            {
                RequestId = request.RequestId,
                CorrelationId = request.CorrelationId,
                EventId = request.EventId,
                ReservationId = reservationId,
                Status = status,
                InstanceId = "li-01",
                SystemId = "li01",
                Endpoint = "https://li-01.internal:7443",
                ReservedCapacity = request.MaximumParticipants
            };
    }

    private sealed class MemoryEventStore(NexusEventDefinitionV1 definition) : IEventStore
    {
        private EventServerAssignment? assignment;
        public bool FailNextAssignmentSave { get; set; }

        public Task<bool> IsReadyAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);
        public Task<NexusEventDefinitionV1?> GetEventAsync(string eventId, CancellationToken cancellationToken = default) =>
            Task.FromResult(eventId == definition.EventId ? definition : null);
        public Task<EventDefinitionWriteResult> CreateEventAsync(NexusEventDefinitionV1 eventDefinition,
            string actorService, CancellationToken cancellationToken = default) =>
            Task.FromResult(new EventDefinitionWriteResult(EventDefinitionWriteStatus.Created, eventDefinition, null));
        public Task<EventRegistrationDecisionV1> RegisterAsync(EventRegistrationRequestV1 request,
            CancellationToken cancellationToken = default) => Task.FromResult(new EventRegistrationDecisionV1());
        public Task<EventLifecycleTransitionDecisionV1> TransitionAsync(EventLifecycleTransitionRequestV1 request,
            string actorId, CancellationToken cancellationToken = default) =>
            Task.FromResult(new EventLifecycleTransitionDecisionV1());
        public Task<EventServerAssignment?> GetEventServerAssignmentAsync(string eventId,
            CancellationToken cancellationToken = default) => Task.FromResult(assignment);

        public Task<bool> SaveEventServerAssignmentAsync(EventServerReservationRequestV1 request,
            EventServerReservationDecisionV1 decision, CancellationToken cancellationToken = default)
        {
            if (FailNextAssignmentSave)
            {
                FailNextAssignmentSave = false;
                return Task.FromResult(false);
            }
            assignment = new EventServerAssignment(request, decision, false, null);
            return Task.FromResult(true);
        }

        public Task<bool> MarkEventServerReservationReleasedAsync(EventServerReservationReleaseRequestV1 request,
            CancellationToken cancellationToken = default)
        {
            if (assignment?.Decision.ReservationId != request.ReservationId) return Task.FromResult(false);
            assignment = assignment with { IsReleased = true, ReleaseIdempotencyKey = request.IdempotencyKey };
            return Task.FromResult(true);
        }

        public Task<IReadOnlyList<NexusEventDefinitionV1>> GetEventsNeedingServerAssignmentAsync(
            CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<NexusEventDefinitionV1>>([]);
        public Task<IReadOnlyList<(EventServerAssignment Assignment, NexusEventPhase Phase)>>
            GetTerminalEventsWithUnreleasedServerAssignmentAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<(EventServerAssignment Assignment, NexusEventPhase Phase)>>([]);
    }
}
