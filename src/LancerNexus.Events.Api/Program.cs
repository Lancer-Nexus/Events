using LancerNexus.Events;
using LancerNexus.Protocol;

var builder = WebApplication.CreateBuilder(args);
var connectionString = builder.Configuration.GetConnectionString("Events") ??
                       builder.Configuration["Events:MySqlConnectionString"];
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<EventServiceCredentialAuthenticator>();
builder.Services.AddSingleton<IEventStore>(services => new MySqlEventStore(connectionString,
    services.GetRequiredService<TimeProvider>()));
builder.Services.AddHttpClient<IEventCoordinatorClient, CoordinatorEventServerClient>(client =>
    client.Timeout = TimeSpan.FromSeconds(8)).ConfigurePrimaryHttpMessageHandler(() =>
    new HttpClientHandler { AllowAutoRedirect = false });
builder.Services.AddScoped<EventServerReservationWorkflow>();
builder.Services.AddHostedService<EventServerReservationRecoveryService>();

var app = builder.Build();

app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
app.MapGet("/health/ready", async (IEventStore store, CancellationToken cancellationToken) =>
    await store.IsReadyAsync(cancellationToken)
        ? Results.Ok(new { status = "ready" })
        : Results.StatusCode(StatusCodes.Status503ServiceUnavailable));
app.MapGet("/api/v1/capabilities", () => Results.Ok(new
{
    service = "events",
    protocolVersion = ProtocolConstants.ProtocolVersion,
    capabilities = new[]
    {
        "health_v1", "event_registration_v1", "event_lifecycle_v1",
        "event_server_assignment_v1", "event_reservation_recovery_v1"
    }
}));

app.MapPost("/internal/v1/events", async (HttpRequest httpRequest, NexusEventDefinitionV1 definition,
    EventServiceCredentialAuthenticator credentials, IEventStore store, CancellationToken cancellationToken) =>
{
    if (!credentials.TryAuthenticate(httpRequest, out var actorService))
        return Results.Unauthorized();
    if (actorService is not ("administration" or "events-admin"))
        return Results.StatusCode(StatusCodes.Status403Forbidden);
    var result = await store.CreateEventAsync(definition, actorService, cancellationToken);
    return result.Status switch
    {
        EventDefinitionWriteStatus.Created => Results.Created($"/internal/v1/events/{definition.EventId}", result.Definition),
        EventDefinitionWriteStatus.AlreadyExists => Results.Ok(result.Definition),
        EventDefinitionWriteStatus.Conflict => Results.Conflict(new { error = result.ErrorCode }),
        EventDefinitionWriteStatus.Invalid => Results.BadRequest(new { error = result.ErrorCode }),
        _ => Results.StatusCode(StatusCodes.Status503ServiceUnavailable)
    };
});

app.MapGet("/internal/v1/events/{eventId}", async (string eventId, HttpRequest httpRequest,
    EventServiceCredentialAuthenticator credentials, IEventStore store, CancellationToken cancellationToken) =>
{
    if (!credentials.TryAuthenticate(httpRequest, out _))
        return Results.Unauthorized();
    if (!AdminQuery.ValidIdentifier(eventId))
        return Results.BadRequest(new { error = "invalid_event_id" });
    try
    {
        var definition = await store.GetEventAsync(eventId, cancellationToken);
        return definition is null ? Results.NotFound() : Results.Ok(definition);
    }
    catch (MySqlConnector.MySqlException)
    {
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    }
});

app.MapPost("/internal/v1/events/{eventId}/registrations", async (string eventId,
    EventRegistrationRequestV1 request, HttpRequest httpRequest,
    EventServiceCredentialAuthenticator credentials, IEventStore store, CancellationToken cancellationToken) =>
{
    if (!credentials.TryAuthenticate(httpRequest, out var callerService))
        return Results.Unauthorized();
    if (callerService != "gateway")
        return Results.StatusCode(StatusCodes.Status403Forbidden);
    if (!string.Equals(eventId, request.EventId, StringComparison.Ordinal) || !request.IsValid())
        return Results.BadRequest(new { error = "invalid_registration_request" });

    var decision = await store.RegisterAsync(request, cancellationToken);
    return decision.Status switch
    {
        EventRegistrationStatus.Accepted or EventRegistrationStatus.AlreadyRegistered => Results.Ok(decision),
        EventRegistrationStatus.Denied => Results.Json(decision, statusCode: StatusCodes.Status403Forbidden),
        EventRegistrationStatus.EventNotFound => Results.NotFound(decision),
        EventRegistrationStatus.RegistrationClosed or EventRegistrationStatus.CapacityFull or
            EventRegistrationStatus.Conflict => Results.Conflict(decision),
        EventRegistrationStatus.Invalid => Results.BadRequest(decision),
        _ => Results.StatusCode(StatusCodes.Status503ServiceUnavailable)
    };
});

app.MapPost("/internal/v1/events/{eventId}/lifecycle", async (string eventId,
    EventLifecycleTransitionRequestV1 request, HttpRequest httpRequest,
    EventServiceCredentialAuthenticator credentials, IEventStore store,
    EventServerReservationWorkflow reservationWorkflow, CancellationToken cancellationToken) =>
{
    if (!credentials.TryAuthenticate(httpRequest, out var callerService))
        return Results.Unauthorized();
    if (!string.Equals(eventId, request.EventId, StringComparison.Ordinal))
        return Results.BadRequest(new { error = "event_id_mismatch" });
    if (!request.IsValid())
        return Results.BadRequest(new { error = "invalid_lifecycle_request" });

    string actorId;
    if (request.ActorAccountId is { } accountId)
    {
        if (callerService != "gateway")
            return Results.StatusCode(StatusCodes.Status403Forbidden);
        actorId = "account:" + accountId.ToString("N");
    }
    else
    {
        if (!string.Equals(request.ActorService, callerService, StringComparison.Ordinal))
            return Results.StatusCode(StatusCodes.Status403Forbidden);
        actorId = "service:" + callerService;
    }

    if (request.NextPhase == NexusEventPhase.Ready)
    {
        var current = await store.GetEventAsync(eventId, cancellationToken);
        if (current?.Phase == NexusEventPhase.Provisioning)
        {
            var reservation = await reservationWorkflow.EnsureReservedAsync(current, cancellationToken);
            if (reservation.Status is not (EventServerReservationStatus.Reserved or
                    EventServerReservationStatus.AlreadyReserved))
                return reservation.Status switch
                {
                    EventServerReservationStatus.NoCapacity or EventServerReservationStatus.Conflict =>
                        Results.Conflict(reservation),
                    EventServerReservationStatus.Invalid => Results.BadRequest(reservation),
                    _ => Results.StatusCode(StatusCodes.Status503ServiceUnavailable)
                };
        }
    }

    var decision = await store.TransitionAsync(request, actorId, cancellationToken);
    if (decision.Status == EventLifecycleStatus.Applied &&
        decision.CurrentPhase == NexusEventPhase.Provisioning)
    {
        var current = await store.GetEventAsync(eventId, cancellationToken);
        if (current is null)
            return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
        var reservation = await reservationWorkflow.EnsureReservedAsync(current, cancellationToken);
        if (reservation.Status is not (EventServerReservationStatus.Reserved or
                EventServerReservationStatus.AlreadyReserved))
            return reservation.Status switch
            {
                EventServerReservationStatus.NoCapacity or EventServerReservationStatus.Conflict =>
                    Results.Conflict(reservation),
                EventServerReservationStatus.Invalid => Results.BadRequest(reservation),
                _ => Results.StatusCode(StatusCodes.Status503ServiceUnavailable)
            };
    }
    if (decision.Status == EventLifecycleStatus.Applied && request.NextPhase is
        NexusEventPhase.Cancelled or NexusEventPhase.Failed or NexusEventPhase.Closed)
    {
        var assignment = await store.GetEventServerAssignmentAsync(eventId, cancellationToken);
        if (assignment is not null &&
            !await reservationWorkflow.EnsureReleasedAsync(assignment, request.NextPhase, cancellationToken))
            return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    }
    return decision.Status switch
    {
        EventLifecycleStatus.Applied => Results.Ok(decision),
        EventLifecycleStatus.Conflict => Results.Conflict(decision),
        EventLifecycleStatus.Denied => Results.StatusCode(StatusCodes.Status403Forbidden),
        EventLifecycleStatus.EventNotFound => Results.NotFound(decision),
        EventLifecycleStatus.InvalidTransition or EventLifecycleStatus.Invalid => Results.BadRequest(decision),
        _ => Results.StatusCode(StatusCodes.Status503ServiceUnavailable)
    };
});

app.Run();

public partial class Program;
