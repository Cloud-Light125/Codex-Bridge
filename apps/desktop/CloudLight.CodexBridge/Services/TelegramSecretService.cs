namespace CloudLight.CodexBridge.Services;

/// <summary>Stores the Telegram token at the v0.3-compatible DPAPI path and entropy.</summary>
public sealed class TelegramSecretService
{
	private readonly DpapiSecretStore _store;

	public TelegramSecretService(string profileId = "telegram-default")
	{
		var defaultProfile = string.Equals(profileId?.Trim(), "telegram-default", StringComparison.OrdinalIgnoreCase);
		var profilePart = ChannelProfileSecretPaths.SafePart(profileId, "telegram-default");
		var fileName = defaultProfile ? "telegram-token.dat" : $"telegram-token-{profilePart}.dat";
		var entropy = defaultProfile
			? "CloudLight.CodexBridge/telegram-token/v1"
			: $"CloudLight.CodexBridge/telegram-token/{profilePart}/v1";
		_store = new DpapiSecretStore(
			Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "CloudLight", "CodexBridge", "secrets", fileName),
			entropy,
			"Telegram Token");
	}

    public string SecretFile => _store.SecretFile;

    public async Task SaveAsync(string token, CancellationToken cancellationToken = default)
    {
        try { await _store.SaveAsync(token, cancellationToken).ConfigureAwait(false); }
        catch (DpapiSecretException exception) { throw new TelegramSecretException(exception.Message, exception); }
    }

    public async Task<string?> LoadAsync(CancellationToken cancellationToken = default)
    {
        try { return await _store.LoadAsync(cancellationToken).ConfigureAwait(false); }
        catch (DpapiSecretException exception) { throw new TelegramSecretException(exception.Message, exception); }
    }

    public async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        try { await _store.DeleteAsync(cancellationToken).ConfigureAwait(false); }
        catch (DpapiSecretException exception) { throw new TelegramSecretException(exception.Message, exception); }
    }
}

internal static class ChannelProfileSecretPaths
{
	public static string SafePart(string? value, string fallback)
	{
		var source = (value ?? "").Trim().ToLowerInvariant();
		var result = new string(source.Select(character => char.IsLetterOrDigit(character) || character is '-' or '_' ? character : '-').ToArray()).Trim('-');
		return result.Length == 0 ? fallback : result[..Math.Min(result.Length, 96)];
	}
}

public sealed class TelegramSecretException(string message, Exception? innerException = null) : Exception(message, innerException)
{
}
