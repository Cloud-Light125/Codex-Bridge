using System.Text.Json;

namespace CloudLight.CodexBridge.Services;

public sealed record OpenClawDiscoveryResult(bool Found, string GatewayUrl, string Token, string Source, string FriendlyName);

/// <summary>
/// Reads OpenClawTray's persisted gateway registry without invoking or
/// changing the Tray. The registry is the observed source of the local
/// Gateway URL and shared token.
/// </summary>
public sealed class OpenClawDiscoveryService(LogService logs)
{
    public string GatewayRegistryPath => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "OpenClawTray", "gateways.json");

    public async Task<OpenClawDiscoveryResult> DiscoverAsync(CancellationToken cancellationToken = default)
    {
        var path = GatewayRegistryPath;
        try
        {
            if (!File.Exists(path)) return new(false, "", "", "None", "");
            await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
            using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (!document.RootElement.TryGetProperty("gateways", out var gateways) || gateways.ValueKind != JsonValueKind.Array)
                return new(false, "", "", "None", "");
            var activeId = document.RootElement.TryGetProperty("activeId", out var active) ? active.GetString() : "";
            JsonElement? selected = null;
            foreach (var gateway in gateways.EnumerateArray())
            {
                if (selected is null) selected = gateway;
                if (!string.IsNullOrWhiteSpace(activeId) && gateway.TryGetProperty("id", out var id) && id.GetString() == activeId)
                {
                    selected = gateway;
                    break;
                }
            }
            if (selected is not JsonElement gatewayElement) return new(false, "", "", "None", "");
            var url = ReadString(gatewayElement, "url");
            var token = ReadString(gatewayElement, "sharedGatewayToken");
            if (!Uri.TryCreate(url, UriKind.Absolute, out var parsed) ||
                (parsed.Scheme != "ws" && parsed.Scheme != "wss") || string.IsNullOrWhiteSpace(parsed.Host) || string.IsNullOrWhiteSpace(token))
                return new(false, "", "", "None", "");
            return new(true, url.Trim(), token.Trim(), "OpenClawTray gateways.json", ReadString(gatewayElement, "friendlyName"));
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or JsonException)
        {
            logs.Add("openclaw-discovery", $"读取 OpenClawTray Gateway 注册表失败：{exception.Message}");
            return new(false, "", "", "None", "");
        }
    }

    private static string ReadString(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() ?? "" : "";
}
