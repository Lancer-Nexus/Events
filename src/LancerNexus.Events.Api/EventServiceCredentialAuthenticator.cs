using System.Security.Cryptography;
using System.Text;
using LancerNexus.Protocol;

namespace LancerNexus.Events;

public sealed class EventServiceCredentialAuthenticator(IConfiguration configuration)
{
    public bool TryAuthenticate(HttpRequest request, out string serviceId)
    {
        serviceId = "";
        var authorization = request.Headers.Authorization.ToString();
        const string prefix = "Bearer ";
        if (!authorization.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return false;
        var supplied = authorization[prefix.Length..];
        var suppliedBytes = Encoding.UTF8.GetBytes(supplied);
        var suppliedHash = SHA256.HashData(suppliedBytes);
        foreach (var entry in configuration.GetSection("Events:ServiceApiKeys").GetChildren())
        {
            if (!AdminQuery.ValidIdentifier(entry.Key) || entry.Value is not { Length: >= 32 } expected)
                continue;
            var expectedHash = SHA256.HashData(Encoding.UTF8.GetBytes(expected));
            if (!CryptographicOperations.FixedTimeEquals(suppliedHash, expectedHash))
                continue;
            serviceId = entry.Key;
            return true;
        }
        return false;
    }
}
