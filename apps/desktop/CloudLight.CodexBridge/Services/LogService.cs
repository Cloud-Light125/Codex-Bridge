using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using CloudLight.CodexBridge.Models;

namespace CloudLight.CodexBridge.Services;

public sealed partial class LogService : IDisposable
{
    private const int MaximumEntries = 1000;
    private const int MaximumQueuedFileWrites = 2048;
    private const int MaximumQueuedViewEntries = 2048;
    private const int MaximumViewBatch = 128;
    private const int MaximumViewEntryCharacters = 64 * 1024;
    private const string TruncatedViewEntryMarker = "\n\n[日志过长，界面已截断；完整内容仍已写入 desktop.log。]";
    private static readonly object FileLock = new();
    private readonly Channel<LogRecord> _fileQueue = Channel.CreateBounded<LogRecord>(new BoundedChannelOptions(MaximumQueuedFileWrites)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.Wait
    });
    private readonly ConcurrentQueue<LogEntry> _viewQueue = new();
    private readonly Task _fileWriter;
    private readonly object _queueGate = new();
    private int _queuedFileWrites;
    private int _queuedViewEntries;
    private int _viewDrainScheduled;
    private long _droppedFileWrites;
    private bool _disposed;

    public string LogDirectory { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "CloudLight", "CodexBridge", "logs");
    public string DesktopLogFile => Path.Combine(LogDirectory, "desktop.log");
    public ObservableCollection<LogEntry> Entries { get; } = [];

    public int PendingFileWrites => Math.Max(0, Volatile.Read(ref _queuedFileWrites));
    public int PendingViewEntries => Math.Max(0, Volatile.Read(ref _queuedViewEntries));
    public long DroppedFileWrites => Interlocked.Read(ref _droppedFileWrites);

    public LogService()
    {
        _fileWriter = Task.Run(WriteFileLoopAsync);
    }

    public void Add(string source, string message)
    {
        var safe = Redact(message);
        EnqueueFileWrite(new LogRecord(source, safe));
        AddToView(source, LimitViewEntry(safe));
    }

    public void AddException(string source, string context, Exception exception)
    {
        var safe = Redact($"{context}{Environment.NewLine}{exception}");
        EnqueueFileWrite(new LogRecord(source, safe));
        AddToView(source, LimitViewEntry(safe));
    }

    public void Clear()
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher is null) return;
        void ClearCore()
        {
            while (_viewQueue.TryDequeue(out _)) Interlocked.Decrement(ref _queuedViewEntries);
            Entries.Clear();
        }

        if (dispatcher.CheckAccess()) ClearCore();
        else _ = dispatcher.InvokeAsync(ClearCore);
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

    private void EnqueueFileWrite(LogRecord record)
    {
        lock (_queueGate)
        {
            if (_disposed) return;
            if (_fileQueue.Writer.TryWrite(record))
            {
                Interlocked.Increment(ref _queuedFileWrites);
                return;
            }

            // Drop the oldest queued record when the writer is behind. The
            // queue is deliberately finite: a logging storm must not retain
            // every message, closure, and Task until the disk catches up.
            if (_fileQueue.Reader.TryRead(out _))
            {
                Interlocked.Decrement(ref _queuedFileWrites);
                Interlocked.Increment(ref _droppedFileWrites);
            }

            if (_fileQueue.Writer.TryWrite(record))
                Interlocked.Increment(ref _queuedFileWrites);
            else
                Interlocked.Increment(ref _droppedFileWrites);
        }
    }

    private void AddToView(string source, string safe)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher is null || _disposed) return;

        _viewQueue.Enqueue(new LogEntry(DateTimeOffset.Now, source, safe));
        Interlocked.Increment(ref _queuedViewEntries);
        while (Volatile.Read(ref _queuedViewEntries) > MaximumQueuedViewEntries && _viewQueue.TryDequeue(out _))
        {
            Interlocked.Decrement(ref _queuedViewEntries);
        }

        if (Interlocked.Exchange(ref _viewDrainScheduled, 1) != 0) return;
        try
        {
            _ = dispatcher.BeginInvoke(new Action(DrainViewQueue));
        }
        catch (Exception) when (dispatcher.HasShutdownStarted || dispatcher.HasShutdownFinished)
        {
            Interlocked.Exchange(ref _viewDrainScheduled, 0);
        }
    }

    private static string LimitViewEntry(string value) => value.Length <= MaximumViewEntryCharacters
        ? value
        : value[..MaximumViewEntryCharacters] + TruncatedViewEntryMarker;

    private void DrainViewQueue()
    {
        try
        {
            var processed = 0;
            while (processed++ < MaximumViewBatch && _viewQueue.TryDequeue(out var entry))
            {
                Interlocked.Decrement(ref _queuedViewEntries);
                Entries.Add(entry);
            }
            while (Entries.Count > MaximumEntries) Entries.RemoveAt(0);
        }
        finally
        {
            Interlocked.Exchange(ref _viewDrainScheduled, 0);
            if (Volatile.Read(ref _queuedViewEntries) > 0 && Application.Current?.Dispatcher is { HasShutdownStarted: false, HasShutdownFinished: false } dispatcher)
                AddViewDrain(dispatcher);
        }
    }

    private void AddViewDrain(System.Windows.Threading.Dispatcher dispatcher)
    {
        if (Interlocked.Exchange(ref _viewDrainScheduled, 1) != 0) return;
        try { _ = dispatcher.BeginInvoke(new Action(DrainViewQueue)); }
        catch (Exception) when (dispatcher.HasShutdownStarted || dispatcher.HasShutdownFinished) { Interlocked.Exchange(ref _viewDrainScheduled, 0); }
    }

    private async Task WriteFileLoopAsync()
    {
        try
        {
            await foreach (var record in _fileQueue.Reader.ReadAllAsync())
            {
                Interlocked.Decrement(ref _queuedFileWrites);
                AppendToDesktopLog(record.Source, record.Message);
            }
        }
        catch (Exception)
        {
            // Logging must never fail the desktop process. Individual writes
            // are already isolated; this protects the final drain as well.
        }
    }

    public void Dispose()
    {
        lock (_queueGate)
        {
            if (_disposed) return;
            _disposed = true;
            _fileQueue.Writer.TryComplete();
        }
        try { _fileWriter.GetAwaiter().GetResult(); }
        catch (Exception) { }
        GC.SuppressFinalize(this);
    }

    private readonly record struct LogRecord(string Source, string Message);

    [GeneratedRegex("(?i)(authorization\\s*:\\s*bearer\\s+)[^\\s,;]+")]
    private static partial Regex AuthorizationRegex();

    [GeneratedRegex("(?i)(bearer\\s+)[A-Za-z0-9._~+\\-/=]+")]
    private static partial Regex BearerRegex();

    [GeneratedRegex("(?i)((?:api[_-]?key|bot[_-]?token|access[_-]?token|refresh[_-]?token|app[_-]?secret|client[_-]?secret|secret|token|password)[\"']?\\s*[=:]\\s*[\"']?)[^\\s,;\"']+")]
    private static partial Regex CredentialRegex();

    [GeneratedRegex("\\bsk-[A-Za-z0-9_-]{12,}\\b")]
    private static partial Regex SecretKeyRegex();

    [GeneratedRegex("\\b[0-9]{6,15}:[A-Za-z0-9_-]{20,}\\b")]
    private static partial Regex TelegramTokenRegex();
}
