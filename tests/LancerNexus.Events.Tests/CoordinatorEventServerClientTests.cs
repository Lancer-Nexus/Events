using System.Net;
using System.Net.Http.Json;
using LancerNexus.Events;
using LancerNexus.Protocol;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace LancerNexus.Events.Tests;

public sealed class CoordinatorEventServerClientTests
{
    [Fact]
    public async Task ReserveAsync_UsesConfiguredHttpsBearerAndTypedRequest()
    {
        HttpMethod? sentMethod = null;
        string? sentUri = null;
        string? sentAuthorization = null;
        string? sentBody = null;
        var request = ReservationRequest();
        var expected = new EventServerReservationDecisionV1
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
        using var http = new HttpClient(new CallbackHandler(message =>
        {
            if (message.Method == HttpMethod.Get)
                return CapabilitiesResponse("event_server_reservations_v1");
            sentMethod = message.Method;
            sentUri = message.RequestUri!.AbsoluteUri;
            sentAuthorization = message.Headers.Authorization!.Parameter;
            sentBody = message.Content!.ReadAsStringAsync().GetAwaiter().GetResult();
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = JsonContent.Create(expected) };
        }));
        var client = CreateClient(http, "https://coordinator.internal", new string('k', 40));

        var actual = await client.ReserveAsync(request);

        Assert.Equal(expected, actual);
        Assert.Equal(HttpMethod.Post, sentMethod);
        Assert.Equal("https://coordinator.internal/internal/v1/events/server-reservations", sentUri);
        Assert.Equal(new string('k', 40), sentAuthorization);
        var actualRequest = System.Text.Json.JsonSerializer.Deserialize<EventServerReservationRequestV1>(sentBody!,
            new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web));
        Assert.Equal(request, actualRequest);
    }

    [Fact]
    public async Task ReserveAsync_RequiresMatchingCoordinatorCapabilityBeforePosting()
    {
        var requestCount = 0;
        using var http = new HttpClient(new CallbackHandler(message =>
        {
            requestCount++;
            return message.Method == HttpMethod.Get
                ? CapabilitiesResponse("event_registration_v1")
                : new HttpResponseMessage(HttpStatusCode.OK);
        }));
        var client = CreateClient(http, "https://coordinator.internal", new string('c', 40));

        var result = await client.ReserveAsync(ReservationRequest());

        Assert.Equal(EventServerReservationStatus.Unavailable, result.Status);
        Assert.Equal(1, requestCount);
    }

    [Theory]
    [InlineData("http://coordinator.internal", "valid-api-key-which-is-long-enough-123456")]
    [InlineData("https://coordinator.internal", "short")]
    public async Task ReserveAsync_FailsClosedForInvalidCoordinatorConfiguration(string baseUrl, string apiKey)
    {
        var sendCount = 0;
        using var http = new HttpClient(new CallbackHandler(_ =>
        {
            sendCount++;
            return new HttpResponseMessage(HttpStatusCode.OK);
        }));
        var client = CreateClient(http, baseUrl, apiKey);

        var result = await client.ReserveAsync(ReservationRequest());

        Assert.Equal(EventServerReservationStatus.Unavailable, result.Status);
        Assert.Equal("coordinator_unavailable", result.ErrorCode);
        Assert.Equal(0, sendCount);
    }

    [Fact]
    public async Task ReleaseAsync_ReturnsCoordinatorReplayDecision()
    {
        var request = new EventServerReservationReleaseRequestV1
        {
            RequestId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            EventId = "event-client-release",
            ReservationId = Guid.NewGuid(),
            IdempotencyKey = "release-key",
            ReasonCode = "event_closed"
        };
        var expected = new EventServerReservationReleaseDecisionV1
        {
            RequestId = request.RequestId,
            CorrelationId = request.CorrelationId,
            EventId = request.EventId,
            ReservationId = request.ReservationId,
            Status = EventServerReservationReleaseStatus.AlreadyReleased
        };
        using var capabilityHttp = new HttpClient(new CallbackHandler(message => message.Method == HttpMethod.Get
            ? CapabilitiesResponse("event_server_reservations_v1")
            : new HttpResponseMessage(HttpStatusCode.OK) { Content = JsonContent.Create(expected) }));
        var client = CreateClient(capabilityHttp, "https://coordinator.internal", new string('r', 40));

        Assert.Equal(expected, await client.ReleaseAsync(request));
    }

    private static CoordinatorEventServerClient CreateClient(HttpClient http, string baseUrl, string apiKey)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Events:Coordinator:BaseUrl"] = baseUrl,
            ["Events:Coordinator:ApiKey"] = apiKey
        }).Build();
        return new CoordinatorEventServerClient(http, configuration);
    }

    private static HttpResponseMessage CapabilitiesResponse(string capability) => new(HttpStatusCode.OK)
    {
        Content = JsonContent.Create(new
        {
            service = "coordinator",
            protocolVersion = ProtocolConstants.ProtocolVersion,
            capabilities = new[] { capability }
        })
    };

    private static EventServerReservationRequestV1 ReservationRequest() => new()
    {
        RequestId = Guid.NewGuid(),
        CorrelationId = Guid.NewGuid(),
        EventId = "event-client-test",
        EventRevision = 3,
        TargetSystemId = "li01",
        MaximumParticipants = 8,
        StartsUtc = DateTimeOffset.Parse("2026-11-01T12:00:00Z"),
        EndsUtc = DateTimeOffset.Parse("2026-11-01T14:00:00Z"),
        IdempotencyKey = "reservation-key"
    };

    private sealed class CallbackHandler(Func<HttpRequestMessage, HttpResponseMessage> callback)
        : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken) => Task.FromResult(callback(request));
    }
}
