using LancerNexus.Events;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace LancerNexus.Events.Tests;

public sealed class EventServiceCredentialAuthenticatorTests
{
    [Fact]
    public void AuthenticatesConfiguredServiceAndReturnsItsIdentity()
    {
        var authenticator = new EventServiceCredentialAuthenticator(new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Events:ServiceApiKeys:gateway"] = new string('g', 40),
                ["Events:ServiceApiKeys:administration"] = new string('a', 40)
            }).Build());
        var request = new DefaultHttpContext().Request;
        request.Headers.Authorization = "Bearer " + new string('a', 40);

        Assert.True(authenticator.TryAuthenticate(request, out var serviceId));
        Assert.Equal("administration", serviceId);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("Basic secret")]
    [InlineData("Bearer wrong")]
    public void RejectsMissingOrInvalidCredential(string? authorization)
    {
        var authenticator = new EventServiceCredentialAuthenticator(new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Events:ServiceApiKeys:gateway"] = new string('g', 40)
            }).Build());
        var request = new DefaultHttpContext().Request;
        if (authorization is not null) request.Headers.Authorization = authorization;

        Assert.False(authenticator.TryAuthenticate(request, out _));
    }
}
