using System.Text.Json;

namespace CloudLight.CodexBridge.Services;

/// <summary>Stores the optional OpenClaw token/password in a Bridge-owned DPAPI file.</summary>
public sealed class OpenClawSecretService
{
    private readonly DpapiSecretStore _store = new(
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "CloudLight", "CodexBridge", "secrets", "openclaw-gateway.dat"),
        "CloudLight.CodexBridge/openclaw-gateway/v1",
        "OpenClaw Gateway 凭据");

    public string SecretFile => _store.SecretFile;

    public async Task SaveAsync(string token, string password, CancellationToken cancellationToken = default)
    {
        token = token?.Trim() ?? "";
        password = password?.Trim() ?? "";
        if (token.Length == 0 && password.Length == 0)
            throw new ArgumentException("OpenClaw Token 或 Password 不能为空。", nameof(token));
        var value = JsonSerializer.Serialize(new Credentials(token, password));
        await _store.SaveAsync(value, cancellationToken).ConfigureAwait(false);
    }

    public async Task<(string Token, string Password)> LoadAsync(CancellationToken cancellationToken = default)
    {
        var value = await _store.LoadAsync(cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(value)) return ("", "");
        try
        {
            var credentials = JsonSerializer.Deserialize<Credentials>(value);
            return (credentials?.Token?.Trim() ?? "", credentials?.Password?.Trim() ?? "");
        }
        catch (JsonException exception)
        {
            throw new DpapiSecretException("已保存的 OpenClaw 凭据文件格式无效。", exception);
        }
    }

    public Task DeleteAsync(CancellationToken cancellationToken = default) => _store.DeleteAsync(cancellationToken);

    private sealed record Credentials(string Token, string Password);
}
