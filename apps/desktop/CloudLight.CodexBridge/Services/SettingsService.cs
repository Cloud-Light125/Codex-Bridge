using System.Text.Json;
using CloudLight.CodexBridge.Models;

namespace CloudLight.CodexBridge.Services;

public sealed class SettingsService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true
    };

    public string DataDirectory { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "CloudLight", "CodexBridge");

    public string LogDirectory => Path.Combine(DataDirectory, "logs");

    public string SettingsFile { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "CloudLight", "CodexBridge", "settings.json");

    public string LastLoadWarning { get; private set; } = "";

    public async Task<UserSettings> LoadAsync()
    {
        var settingsExists = await Task.Run(() =>
        {
            Directory.CreateDirectory(DataDirectory);
            Directory.CreateDirectory(LogDirectory);
            return File.Exists(SettingsFile);
        }).ConfigureAwait(false);
        if (!settingsExists)
        {
            return new UserSettings();
        }
        try
        {
            await using var stream = File.OpenRead(SettingsFile);
            return Normalize(await JsonSerializer.DeserializeAsync<UserSettings>(stream, JsonOptions).ConfigureAwait(false) ?? new UserSettings());
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            var backup = SettingsFile + $".corrupt-{DateTime.Now:yyyyMMdd-HHmmss-fff}.bak";
            try
            {
                File.Copy(SettingsFile, backup, overwrite: false);
                LastLoadWarning = $"settings.json 无法解析，已保留原文件并备份到 {backup}。本次使用默认设置。";
            }
            catch (Exception backupException)
            {
                LastLoadWarning = $"settings.json 无法解析；原文件未覆盖，但备份失败：{backupException.Message}";
            }
            return new UserSettings();
        }
    }

    public async Task SaveAsync(UserSettings settings)
    {
        Normalize(settings);
        var directory = Path.GetDirectoryName(SettingsFile)!;
        await Task.Run(() => Directory.CreateDirectory(directory)).ConfigureAwait(false);
        var temporaryFile = SettingsFile + ".tmp";
        await using (var stream = new FileStream(temporaryFile, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            await JsonSerializer.SerializeAsync(stream, settings, JsonOptions).ConfigureAwait(false);
        }
        File.Move(temporaryFile, SettingsFile, true);
    }

    private static UserSettings Normalize(UserSettings settings)
    {
        settings.CodexCustomPath = settings.CodexCustomPath?.Trim() ?? "";
        settings.DetectedCodexPath = settings.DetectedCodexPath?.Trim() ?? "";
        CodexPathSettings.RemoveUnsafeAutomaticPath(settings);
        settings.TelegramAllowedUserIds ??= [];
        settings.TelegramAllowedUserIds = settings.TelegramAllowedUserIds.Where(id => id > 0).Distinct().ToList();
        settings.TelegramPollingTimeoutSeconds = Math.Clamp(settings.TelegramPollingTimeoutSeconds, 10, 60);
        settings.TelegramProxyMode = NormalizeProxyMode(settings.TelegramProxyMode);
        settings.TelegramProxyUrl = settings.TelegramProxyUrl?.Trim() ?? "";
        if (settings.TelegramProxyMode != "custom-http" || !IsValidHttpProxyUrl(settings.TelegramProxyUrl))
        {
            if (settings.TelegramProxyMode == "custom-http") settings.TelegramProxyMode = "environment";
            settings.TelegramProxyUrl = "";
        }
		settings.QqAppId = settings.QqAppId?.Trim() ?? "";
		settings.QqEnvironment = "production";
		settings.QqAllowedUserOpenIds = NormalizeOpenIds(settings.QqAllowedUserOpenIds);
		settings.QqAllowedGroupOpenIds = NormalizeOpenIds(settings.QqAllowedGroupOpenIds);
		settings.QqAllowedGroupMemberOpenIds = NormalizeOpenIds(settings.QqAllowedGroupMemberOpenIds);
		settings.QqGroupTriggerMode = "official-at";
		settings.QqCommandPrefix = string.IsNullOrWhiteSpace(settings.QqCommandPrefix)
			? "/codex"
			: settings.QqCommandPrefix.Trim();
		settings.QqProxyMode = NormalizeProxyMode(settings.QqProxyMode);
		settings.QqProxyUrl = settings.QqProxyUrl?.Trim() ?? "";
		if (settings.QqProxyMode != "custom-http" || !IsValidHttpProxyUrl(settings.QqProxyUrl))
		{
			if (settings.QqProxyMode == "custom-http") settings.QqProxyMode = "environment";
			settings.QqProxyUrl = "";
		}
		settings.ThreadRefreshIntervalSeconds = Math.Clamp(settings.ThreadRefreshIntervalSeconds, 10, 300);
		settings.Theme = settings.Theme is "light" or "dark" ? settings.Theme : "system";
		settings.LastPage = settings.LastPage is "overview" or "sessions" or "channels" or "mirror" or "backup" or "settings" or "logs"
			? settings.LastPage
			: "overview";
		settings.WindowWidth = Math.Clamp(double.IsFinite(settings.WindowWidth) ? settings.WindowWidth : 1280, 1040, 3840);
		settings.WindowHeight = Math.Clamp(double.IsFinite(settings.WindowHeight) ? settings.WindowHeight : 800, 680, 2160);
		return settings;
	}

	private static List<string> NormalizeOpenIds(IEnumerable<string>? values)
	{
		var normalized = new List<string>();
		var seen = new HashSet<string>(StringComparer.Ordinal);
		foreach (var value in values ?? [])
		{
			var candidate = value?.Trim() ?? "";
			if (candidate.Length is 0 or > 256) continue;
			if (seen.Add(candidate)) normalized.Add(candidate);
		}
		return normalized;
	}

    private static string NormalizeProxyMode(string? mode) => mode?.Trim().ToLowerInvariant() switch
    {
        "direct" => "direct",
        "custom-http" => "custom-http",
        _ => "environment"
    };

    private static bool IsValidHttpProxyUrl(string value) =>
        Uri.TryCreate(value, UriKind.Absolute, out var uri) &&
        string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) &&
        !string.IsNullOrWhiteSpace(uri.Host) &&
        !value.Contains('@') &&
        string.IsNullOrEmpty(uri.UserInfo) &&
        string.IsNullOrEmpty(uri.Query) &&
        string.IsNullOrEmpty(uri.Fragment) &&
        (string.IsNullOrEmpty(uri.AbsolutePath) || uri.AbsolutePath == "/");
}
