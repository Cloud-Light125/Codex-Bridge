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
            "disconnected" => "未连接",
            "stopped" or "disabled" => "已停止",
            "failed" or "error" => "失败",
			"gateway-failed" => "QQ Gateway 连接失败",
            "configured" => "已配置",
            "not-configured" or "unconfigured" => "未配置",
            "reconnecting" or "connecting" => "正在重连",
            "authentication-failed" or "auth-failed" => "凭据无效",
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
        if (normalized.Contains("qqbot_auth") || normalized.Contains("authentication") || normalized.Contains("http 401") || normalized.Contains("unauthorized"))
            return "无法连接 QQ 机器人，请检查应用凭据。";
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
        return string.IsNullOrWhiteSpace(value)
            ? $"{action}未完成，请重试。"
            : $"{action}未完成，请重试。详情已写入运行日志。";
    }

    public static string ConversationType(string? value) => value?.Trim().ToLowerInvariant() switch
    {
        "c2c" => "私聊",
        "group" => "群聊",
        _ => "会话"
    };

    private static bool ContainsChinese(string value) => value.Any(character => character >= '\u4e00' && character <= '\u9fff');
}
