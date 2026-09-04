using System.Globalization;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;

namespace CloudLight.CodexBridge.ViewModels;

internal static class UiText
{
    public const string CodexDiscoveryRetrying = "暂未找到 Codex，正在后台重新检测。";
    public const string CodexDiscoveryNotFound = "暂未找到 Codex。软件会在后续刷新时继续检测，也可以在设置中手动指定路径。";

    public static string LocalDateTime(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return "—";
        return DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture,
            DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeUniversal, out var parsed)
            ? parsed.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture)
            : value;
    }

    public static string Status(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return "未知";
        var normalized = value.Trim().ToLowerInvariant().Replace('_', '-').Replace(' ', '-');
        return normalized switch
        {
            "idle" => "空闲",
            "running" or "running-local" => "运行中",
            "running-external" => "由其他 Codex 客户端运行",
            "waiting" or "waiting-user-input" => "等待回复",
            "waiting-approval" => "等待确认",
			"connected" or "ready" => "已连接",
			"disconnected" or "stopped" => "未连接",
			"disabled" => "未启用",
			"failed" or "error" => "连接失败",
			"gateway-failed" or "gateway-unavailable" => "暂时无法连接 QQ 服务，请稍后重试。",
			"configured" => "已配置",
			"not-configured" or "unconfigured" => "机器人配置暂时不可用，请刷新后重试。",
			"channel-unavailable" => "机器人当前未连接。",
			"profile-unavailable" => "机器人配置暂时不可用，请刷新后重试。",
			"reconnecting" or "connecting" => "正在连接",
			"authentication-failed" or "auth-failed" or "qqbot-auth" => "机器人 ID 或密钥不正确，请检查后重试。",
			"invalid-token" or "token-invalid" or "telegram-token-invalid" => "机器人密钥无效，请检查后重试。",
			"conflict" or "polling-conflict" => "这个 Telegram 机器人正在被其他程序使用，请关闭另一个机器人程序后再连接。",
            "interrupting" => "正在停止",
            "completed" => "已完成",
            "completed-unverified" => "已完成，等待确认保存状态",
            "persisted" => "已保存",
            "persistence-failed" => "保存状态检查失败",
            "thread-mismatch" => "会话不一致",
            "accepted" => "已接收",
            "polling" => "正在接收消息",
            _ when ContainsChinese(value) => value,
            _ => "状态未知"
        };
    }

    public static string UserError(Exception exception, string action = "操作") =>
        UserError(exception is BridgeApiException api ? $"{api.Code} {api.Message}" : exception.Message, action);

    public static string UserError(string? detail, string action = "操作")
    {
        var value = LogService.Redact(detail ?? string.Empty);
        var normalized = value.ToLowerInvariant().Replace('-', '_');
        var actionText = (action ?? string.Empty).ToLowerInvariant();
        if (normalized.Contains("polling_conflict") || normalized.Contains("another getupdates") || normalized.Contains("conflict"))
            return "这个 Telegram 机器人正在被其他程序使用，请关闭另一个机器人程序后再连接。";
        if (normalized.Contains("token_invalid") || normalized.Contains("invalid_token") || normalized.Contains("invalid-token") || normalized.Contains("token invalid") || normalized.Contains("invalid token") || normalized.Contains("telegram bot token is invalid") || normalized.Contains("telegram rejected the bot token"))
            return "机器人密钥无效，请检查后重试。";
        if (normalized.Contains("must_be_stopped") || normalized.Contains("must be stopped") || normalized.Contains("stop qq official bot") || normalized.Contains("stop telegram before"))
            return "机器人正在重新连接，请稍后重试。";
        if (normalized.Contains("channel_profiles_unavailable") || normalized.Contains("channel_profile_not_found") || normalized.Contains("profile_unavailable") || normalized.Contains("profile unavailable"))
            return "机器人配置暂时不可用，请刷新后重试。";
        if (normalized.Contains("channel_unavailable") || normalized.Contains("channel unavailable") || normalized.Contains("adapter is not started") || normalized.Contains("resource is unavailable"))
            return "机器人当前未连接。";
        if (normalized.Contains("gateway_unavailable") || normalized.Contains("gateway unavailable") || normalized.Contains("unable to connect to qq gateway") || normalized.Contains("gateway_lookup"))
            return "暂时无法连接 QQ 服务，请稍后重试。";
        if (normalized.Contains("qqbot_secret_invalid") || normalized.Contains("secret_invalid") || normalized.Contains("appid_invalid") || normalized.Contains("credentials_missing") || normalized.Contains("qqbot_auth") || normalized.Contains("authentication failed") || normalized.Contains("authentication_failed") || normalized.Contains("http 401") || normalized.Contains("unauthorized"))
            return "机器人 ID 或密钥不正确，请检查后重试。";
        if (normalized.Contains("authentication") && (actionText.Contains("qq") || normalized.Contains("appid") || normalized.Contains("appsecret")))
            return "机器人 ID 或密钥不正确，请检查后重试。";
        if (normalized.Contains("permission") || normalized.Contains("forbidden") || normalized.Contains("http 403") || normalized.Contains("intent_not_enabled"))
            return "QQ 机器人权限不足，请检查开放平台中的消息权限。";
        if (normalized.Contains("timeout") || normalized.Contains("timed out") || normalized.Contains("超时"))
            return "连接超时，请检查网络或代理设置。";
        if (normalized.Contains("gateway_lookup") || normalized.Contains("dns") || normalized.Contains("network") || normalized.Contains("proxy") || normalized.Contains("tls"))
            return "无法建立连接，请检查网络或代理设置。";
        if (normalized.Contains("protocol_incompatible"))
            return "当前版本与 QQ 服务不兼容，请查看运行日志并升级软件。";
        if (normalized.Contains("object reference") || normalized.Contains("nullreference"))
            return $"{action}未完成，请重试。详情已写入运行日志。";
        if (normalized.Contains("persistence") || normalized.Contains("persisted"))
            return "无法确认会话保存状态，请重新检查会话。";
        if (normalized.Contains("backend") || normalized.Contains("api_error") || normalized.Contains("not ready") || normalized.Contains("尚未连接本地"))
            return "本地服务尚未就绪，请稍后重试。";
        if (ContainsChinese(value)) return value;
        return string.IsNullOrWhiteSpace(value)
            ? $"{action}未完成，请重试。"
            : $"{action}未完成，请重试。详情已写入运行日志。";
    }

	public static string ConversationType(string? value) => value?.Trim().ToLowerInvariant() switch
	{
		"c2c" or "private" => "私聊",
		"group" or "supergroup" or "channel" => "群聊",
		_ => "会话"
	};

    private static bool ContainsChinese(string value) => value.Any(character => character >= '\u4e00' && character <= '\u9fff');
}
