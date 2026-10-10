using LancerNexus.Protocol;

namespace LancerNexus.Events;

public enum EventDefinitionWriteStatus { Created, AlreadyExists, Conflict, Invalid, Unavailable }

public sealed record EventDefinitionWriteResult(EventDefinitionWriteStatus Status,
    NexusEventDefinitionV1? Definition, string? ErrorCode);

public sealed record EventServerAssignment(
    EventServerReservationRequestV1 Request,
    EventServerReservationDecisionV1 Decision,
    bool IsReleased,
    string? ReleaseIdempotencyKey);

public interface IEventStore
{
    Task<bool> IsReadyAsync(CancellationToken cancellationToken = default);
    Task<NexusEventDefinitionV1?> GetEventAsync(string eventId, CancellationToken cancellationToken = default);
    Task<EventDefinitionWriteResult> CreateEventAsync(NexusEventDefinitionV1 definition, string actorService,
        CancellationToken cancellationToken = default);
    Task<EventRegistrationDecisionV1> RegisterAsync(EventRegistrationRequestV1 request,
        CancellationToken cancellationToken = default);
    Task<EventLifecycleTransitionDecisionV1> TransitionAsync(EventLifecycleTransitionRequestV1 request,
        string actorId, CancellationToken cancellationToken = default);
    Task<EventServerAssignment?> GetEventServerAssignmentAsync(string eventId,
        CancellationToken cancellationToken = default);
    Task<bool> SaveEventServerAssignmentAsync(EventServerReservationRequestV1 request,
        EventServerReservationDecisionV1 decision, CancellationToken cancellationToken = default);
    Task<bool> MarkEventServerReservationReleasedAsync(EventServerReservationReleaseRequestV1 request,
        CancellationToken cancellationToken = default);
    Task<IReadOnlyList<NexusEventDefinitionV1>> GetEventsNeedingServerAssignmentAsync(
        CancellationToken cancellationToken = default);
    Task<IReadOnlyList<(EventServerAssignment Assignment, NexusEventPhase Phase)>>
        GetTerminalEventsWithUnreleasedServerAssignmentAsync(CancellationToken cancellationToken = default);
}
