using System.Collections.ObjectModel;
using System.Text;
using System.Text.RegularExpressions;
using CloudLight.CodexBridge.Models;

namespace CloudLight.CodexBridge.Services;

public sealed partial class LogService
{
    private const int MaximumEntries = 1000;
    private static readonly object FileLock = new();
    public string LogDirectory { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "CloudLight", "CodexBridge", "logs");
    public string DesktopLogFile => Path.Combine(LogDirectory, "desktop.log");
    public ObservableCollection<LogEntry> Entries { get; } = [];

    public void Add(string source, string message)
    {
        var safe = Redact(message);
        _ = Task.Run(() => AppendToDesktopLog(source, safe));
        AddToView(source, safe);
    }

    public void AddException(string source, string context, Exception exception)
    {
        var safe = Redact($"{context}{Environment.NewLine}{exception}");
        // Fatal handlers may terminate the process immediately, so persist these synchronously.
        AppendToDesktopLog(source, safe);
        AddToView(source, safe);
    }

    public void Clear()
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher is null) return;
        if (dispatcher.CheckAccess()) Entries.Clear();
        else _ = dispatcher.InvokeAsync(Entries.Clear);
    }

    public static string Redact(string value)
    {
        var result = AuthorizationRegex().Replace(value, "$1[REDACTED]");
        result = BearerRegex().Replace(result, "$1[REDACTED]");
        result = CredentialRegex().Replace(result, "$1[REDACTED]");
        result = SecretKeyRegex().Replace(result, "[REDACTED]");
        return TelegramTokenRegex().Replace(result, "[REDACTED]");
    }

    private void AppendToDesktopLog(string source, string message)
    {
        try
        {
            lock (FileLock)
            {
                Directory.CreateDirectory(LogDirectory);
                File.AppendAllText(
                    DesktopLogFile,
                    $"{DateTimeOffset.Now:O} [{source}] {message}{Environment.NewLine}",
                    Encoding.UTF8);
            }
        }
        catch
        {
            // Logging must never cause another application failure.
        }
    }

    private void AddToView(string source, string safe)
    {
        void AddCore()
        {
            Entries.Add(new LogEntry(DateTimeOffset.Now, source, safe));
            while (Entries.Count > MaximumEntries) Entries.RemoveAt(0);
        }

        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher is null) return;
        if (dispatcher.CheckAccess()) AddCore();
        else _ = dispatcher.InvokeAsync(AddCore);
    }

    [GeneratedRegex("(?i)(authorization\\s*:\\s*bearer\\s+)[^\\s,;]+")]
    private static partial Regex AuthorizationRegex();

    [GeneratedRegex("(?i)(bearer\\s+)[A-Za-z0-9._~+\\-/=]+")]
    private static partial Regex BearerRegex();

    [GeneratedRegex("(?i)((?:api[_-]?key|bot[_-]?token|access[_-]?token|refresh[_-]?token|token|password)[\"']?\\s*[=:]\\s*[\"']?)[^\\s,;\"']+")]
    private static partial Regex CredentialRegex();

    [GeneratedRegex("\\bsk-[A-Za-z0-9_-]{12,}\\b")]
    private static partial Regex SecretKeyRegex();

    [GeneratedRegex("\\b[0-9]{6,15}:[A-Za-z0-9_-]{20,}\\b")]
    private static partial Regex TelegramTokenRegex();
}
