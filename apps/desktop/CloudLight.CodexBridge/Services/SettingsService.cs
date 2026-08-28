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

    // Exposed for deterministic migration verification.  It performs only
    // in-memory normalization and never reads or writes a user's settings.
    public static UserSettings NormalizeForMigration(UserSettings settings) => Normalize(settings ?? new UserSettings());

    private static UserSettings Normalize(UserSettings settings)
    {
        settings.CodexCustomPath = settings.CodexCustomPath?.Trim() ?? "";
        settings.DetectedCodexPath = settings.DetectedCodexPath?.Trim() ?? "";
		settings.OpenClawGatewayUrl = NormalizeOpenClawUrl(settings.OpenClawGatewayUrl);
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
		NormalizeChannelProfiles(settings);
		settings.ThreadRefreshIntervalSeconds = Math.Clamp(settings.ThreadRefreshIntervalSeconds, 10, 300);
		settings.Theme = settings.Theme is "light" or "dark" ? settings.Theme : "system";
		settings.LastPage = settings.LastPage is "overview" or "sessions" or "openclaw" or "qq" or "telegram" or "channels" or "commands" or "mirror" or "backup" or "settings" or "logs"
			? settings.LastPage
			: "overview";
		settings.WindowWidth = Math.Clamp(double.IsFinite(settings.WindowWidth) ? settings.WindowWidth : 1280, 1040, 3840);
		settings.WindowHeight = Math.Clamp(double.IsFinite(settings.WindowHeight) ? settings.WindowHeight : 800, 680, 2160);
		return settings;
	}

	// v1 stored a single Telegram and QQ configuration directly on
	// UserSettings.  Lift it into stable default profiles on load so existing
	// installations keep their DPAPI secrets and don't have to reconfigure.
	private static void NormalizeChannelProfiles(UserSettings settings)
	{
		settings.ChannelProfiles ??= [];
		if (!settings.ChannelProfilesMigrated && settings.ChannelProfiles.Count == 0)
		{
			settings.ChannelProfiles =
			[
				new ChannelProfileSettings
				{
					Id = "telegram-default", Name = "Telegram-1", Platform = "telegram", Enabled = true,
					Telegram = new TelegramProfileSettings
					{
						AllowedUserIds = [.. settings.TelegramAllowedUserIds], PollingTimeoutSeconds = settings.TelegramPollingTimeoutSeconds,
						SendProgressUpdates = settings.TelegramSendProgressUpdates, AutoStart = settings.TelegramAutoStart,
						ProxyMode = settings.TelegramProxyMode, ProxyUrl = settings.TelegramProxyUrl
					}
				},
				new ChannelProfileSettings
				{
					Id = "qq-default", Name = "QQ-1", Platform = "qqbot", Enabled = true,
					Qq = new QqProfileSettings
					{
						AppId = settings.QqAppId, AutoStart = settings.QqAutoStart, ReconnectEnabled = settings.QqReconnectEnabled,
						SendProgressUpdates = settings.QqSendProgressUpdates, AllowedUserOpenIds = [.. settings.QqAllowedUserOpenIds],
						AllowedGroupOpenIds = [.. settings.QqAllowedGroupOpenIds], AllowedGroupMemberOpenIds = [.. settings.QqAllowedGroupMemberOpenIds],
						GroupTriggerMode = settings.QqGroupTriggerMode, CommandPrefix = settings.QqCommandPrefix,
						ProxyMode = settings.QqProxyMode, ProxyUrl = settings.QqProxyUrl
					}
				}
			];
		}
		settings.ChannelProfilesMigrated = true;

		var usedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		var normalized = new List<ChannelProfileSettings>();
		foreach (var profile in settings.ChannelProfiles)
		{
			if (profile is null) continue;
			profile.Platform = profile.Platform?.Trim().ToLowerInvariant() == "qqbot" ? "qqbot" : "telegram";
			var fallbackId = profile.Platform == "qqbot" ? "qq-profile" : "telegram-profile";
			profile.Id = NormalizeProfileId(profile.Id, fallbackId, usedIds);
			profile.Name = string.IsNullOrWhiteSpace(profile.Name) ? profile.Id : profile.Name.Trim();
			profile.Telegram ??= new TelegramProfileSettings();
			profile.Qq ??= new QqProfileSettings();
			profile.Telegram.AllowedUserIds = (profile.Telegram.AllowedUserIds ?? []).Where(id => id > 0).Distinct().ToList();
			profile.Telegram.PollingTimeoutSeconds = Math.Clamp(profile.Telegram.PollingTimeoutSeconds, 10, 60);
			profile.Telegram.ProxyMode = NormalizeProxyMode(profile.Telegram.ProxyMode);
			profile.Telegram.ProxyUrl = NormalizeProfileProxy(profile.Telegram.ProxyMode, profile.Telegram.ProxyUrl, out var telegramMode);
			profile.Telegram.ProxyMode = telegramMode;
			profile.Qq.AppId = profile.Qq.AppId?.Trim() ?? "";
			profile.Qq.AllowedUserOpenIds = NormalizeOpenIds(profile.Qq.AllowedUserOpenIds);
			profile.Qq.AllowedGroupOpenIds = NormalizeOpenIds(profile.Qq.AllowedGroupOpenIds);
			profile.Qq.AllowedGroupMemberOpenIds = NormalizeOpenIds(profile.Qq.AllowedGroupMemberOpenIds);
			profile.Qq.GroupTriggerMode = "official-at";
			profile.Qq.CommandPrefix = string.IsNullOrWhiteSpace(profile.Qq.CommandPrefix) ? "/codex" : profile.Qq.CommandPrefix.Trim();
			profile.Qq.ProxyMode = NormalizeProxyMode(profile.Qq.ProxyMode);
			profile.Qq.ProxyUrl = NormalizeProfileProxy(profile.Qq.ProxyMode, profile.Qq.ProxyUrl, out var qqMode);
			profile.Qq.ProxyMode = qqMode;
			normalized.Add(profile);
		}
		settings.ChannelProfiles = normalized;

		settings.ChannelRouting ??= new BackendChannelRoutingSettings();
		settings.ChannelRouting.Codex ??= new BackendChannelRouteSettings();
		settings.ChannelRouting.OpenClaw ??= new BackendChannelRouteSettings();
		var telegramIds = settings.ChannelProfiles.Where(item => item.Platform == "telegram").Select(item => item.Id).ToHashSet(StringComparer.OrdinalIgnoreCase);
		var qqIds = settings.ChannelProfiles.Where(item => item.Platform == "qqbot").Select(item => item.Id).ToHashSet(StringComparer.OrdinalIgnoreCase);
		settings.ChannelRouting.Codex.TelegramProfileIds = NormalizeRouteIds(settings.ChannelRouting.Codex.TelegramProfileIds, telegramIds);
		settings.ChannelRouting.Codex.QqProfileIds = NormalizeRouteIds(settings.ChannelRouting.Codex.QqProfileIds, qqIds);
		settings.ChannelRouting.OpenClaw.TelegramProfileIds = NormalizeRouteIds(settings.ChannelRouting.OpenClaw.TelegramProfileIds, telegramIds);
		settings.ChannelRouting.OpenClaw.QqProfileIds = NormalizeRouteIds(settings.ChannelRouting.OpenClaw.QqProfileIds, qqIds);
		if (settings.ChannelRouting.Codex.TelegramProfileIds.Count == 0 && telegramIds.Contains("telegram-default")) settings.ChannelRouting.Codex.TelegramProfileIds.Add("telegram-default");
		if (settings.ChannelRouting.OpenClaw.TelegramProfileIds.Count == 0 && telegramIds.Contains("telegram-default")) settings.ChannelRouting.OpenClaw.TelegramProfileIds.Add("telegram-default");
		if (settings.ChannelRouting.Codex.QqProfileIds.Count == 0 && qqIds.Contains("qq-default")) settings.ChannelRouting.Codex.QqProfileIds.Add("qq-default");
		if (settings.ChannelRouting.OpenClaw.QqProfileIds.Count == 0 && qqIds.Contains("qq-default")) settings.ChannelRouting.OpenClaw.QqProfileIds.Add("qq-default");
	}

	private static string NormalizeProfileId(string? value, string fallback, ISet<string> used)
	{
		var candidate = new string((value ?? "").Trim().ToLowerInvariant().Select(character => char.IsLetterOrDigit(character) || character is '-' or '_' ? character : '-').ToArray()).Trim('-');
		if (candidate.Length == 0) candidate = fallback;
		if (candidate.Length > 96) candidate = candidate[..96];
		var unique = candidate;
		var suffix = 2;
		while (!used.Add(unique)) unique = $"{candidate}-{suffix++}";
		return unique;
	}

	private static List<string> NormalizeRouteIds(IEnumerable<string>? values, ISet<string> allowed)
	{
		var result = new List<string>();
		var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		foreach (var value in values ?? [])
		{
			var id = value?.Trim() ?? "";
			if (allowed.Contains(id) && seen.Add(id)) result.Add(id);
		}
		return result;
	}

	private static string NormalizeProfileProxy(string mode, string? value, out string normalizedMode)
	{
		normalizedMode = mode;
		var candidate = value?.Trim() ?? "";
		if (normalizedMode != "custom-http" || !IsValidHttpProxyUrl(candidate))
		{
			if (normalizedMode == "custom-http") normalizedMode = "environment";
			return "";
		}
		return candidate;
	}

	private static string NormalizeOpenClawUrl(string? value)
	{
		var candidate = value?.Trim() ?? "";
		if (candidate.Length == 0) return "ws://127.0.0.1:18789";
		return Uri.TryCreate(candidate, UriKind.Absolute, out var uri) &&
			(uri.Scheme.Equals("ws", StringComparison.OrdinalIgnoreCase) || uri.Scheme.Equals("wss", StringComparison.OrdinalIgnoreCase)) &&
			!string.IsNullOrWhiteSpace(uri.Host) ? candidate : "ws://127.0.0.1:18789";
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
