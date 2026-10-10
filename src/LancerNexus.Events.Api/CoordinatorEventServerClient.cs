using System.Net;
using System.Net.Http.Json;
using LancerNexus.Protocol;

namespace LancerNexus.Events;

public interface IEventCoordinatorClient
{
    Task<EventServerReservationDecisionV1> ReserveAsync(EventServerReservationRequestV1 request,
        CancellationToken cancellationToken = default);
    Task<EventServerReservationReleaseDecisionV1> ReleaseAsync(EventServerReservationReleaseRequestV1 request,
        CancellationToken cancellationToken = default);
}

public sealed class CoordinatorEventServerClient(HttpClient httpClient, IConfiguration configuration)
    : IEventCoordinatorClient
{
    private volatile bool coordinatorCapabilityVerified;

    public async Task<EventServerReservationDecisionV1> ReserveAsync(EventServerReservationRequestV1 request,
        CancellationToken cancellationToken = default) =>
        await SendAsync<EventServerReservationRequestV1, EventServerReservationDecisionV1>(
            request, "internal/v1/events/server-reservations", cancellationToken) ??
        new EventServerReservationDecisionV1
        {
            RequestId = request.RequestId,
            CorrelationId = request.CorrelationId,
            EventId = request.EventId,
            Status = EventServerReservationStatus.Unavailable,
            ErrorCode = "coordinator_unavailable"
        };

    public async Task<EventServerReservationReleaseDecisionV1> ReleaseAsync(
        EventServerReservationReleaseRequestV1 request, CancellationToken cancellationToken = default) =>
        await SendAsync<EventServerReservationReleaseRequestV1, EventServerReservationReleaseDecisionV1>(
            request, "internal/v1/events/server-reservations/release", cancellationToken) ??
        new EventServerReservationReleaseDecisionV1
        {
            RequestId = request.RequestId,
            CorrelationId = request.CorrelationId,
            EventId = request.EventId,
            Status = EventServerReservationReleaseStatus.Unavailable,
            ErrorCode = "coordinator_unavailable"
        };

    private async Task<TResponse?> SendAsync<TRequest, TResponse>(TRequest request, string path,
        CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        var baseUrl = configuration["Events:Coordinator:BaseUrl"];
        var apiKey = configuration["Events:Coordinator:ApiKey"];
        if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var baseUri) || baseUri.Scheme != Uri.UriSchemeHttps ||
            baseUri.AbsolutePath != "/" || !string.IsNullOrEmpty(baseUri.UserInfo) ||
            !string.IsNullOrEmpty(baseUri.Query) || !string.IsNullOrEmpty(baseUri.Fragment) ||
            string.IsNullOrWhiteSpace(apiKey) || System.Text.Encoding.UTF8.GetByteCount(apiKey) < 32)
            return null;

        try
        {
            if (!coordinatorCapabilityVerified)
            {
                using var capabilityResponse = await httpClient.GetAsync(new Uri(baseUri, "api/v1/capabilities"),
                    cancellationToken);
                if (!capabilityResponse.IsSuccessStatusCode)
                    return null;
                var capabilities = await capabilityResponse.Content.ReadFromJsonAsync<CoordinatorCapabilities>(
                    cancellationToken);
                if (capabilities is null || capabilities.Service != "coordinator" ||
                    capabilities.ProtocolVersion != ProtocolConstants.ProtocolVersion ||
                    capabilities.Capabilities is null ||
                    !capabilities.Capabilities.Contains("event_server_reservations_v1", StringComparer.Ordinal))
                    return null;
                coordinatorCapabilityVerified = true;
            }

            using var message = new HttpRequestMessage(HttpMethod.Post, new Uri(baseUri, path))
            {
                Content = JsonContent.Create(request)
            };
            message.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", apiKey);
            using var response = await httpClient.SendAsync(message, cancellationToken);
            if (response.StatusCode is not (HttpStatusCode.OK or HttpStatusCode.BadRequest or
                    HttpStatusCode.Conflict or HttpStatusCode.NotFound or HttpStatusCode.ServiceUnavailable))
                return null;
            var decision = await response.Content.ReadFromJsonAsync<TResponse>(cancellationToken);
            return decision;
        }
        catch (HttpRequestException)
        {
            return null;
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
        catch (TaskCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return null;
        }
    }

    private sealed record CoordinatorCapabilities(string Service, int ProtocolVersion, string[] Capabilities);
}
