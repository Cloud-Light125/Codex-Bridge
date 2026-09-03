using System.Collections.Concurrent;
using System.Diagnostics;

namespace CloudLight.CodexBridge.Services;

public enum CodexDiscoverySource
{
    None,
    Manual,
    SavedPath,
    PATH,
    CodexProcess,
    ChatGPTProcess
}

public sealed record CodexDiscoveryResult(
    bool Found,
    string Path,
    string Version,
    CodexDiscoverySource Source)
{
    public string RuntimeSource => Source switch
    {
        CodexDiscoverySource.CodexProcess => "RunningCodex",
        CodexDiscoverySource.ChatGPTProcess => "RunningChatGPT",
        CodexDiscoverySource.Manual => "Manual",
        CodexDiscoverySource.PATH => "PATH",
        CodexDiscoverySource.SavedPath => "SavedPath",
        _ => ""
    };
}

public sealed class CodexDiscoveryService(LogService logs)
{
    private static readonly string[] CodexFileNames = ["codex.exe", "codex", "codex.cmd", "codex.bat"];
    private static readonly string[] CommonChatGPTRelativeDirectories = ["", "resources", "bin", "cli", "tools"];
    private readonly ConcurrentDictionary<string, DateTimeOffset> _recentDiagnostics = new(StringComparer.Ordinal);
    private const int MaximumSearchDepth = 6;
    private const int MaximumSearchedDirectories = 1024;
    private const int MaximumRecentDiagnostics = 512;

    public Task<CodexDiscoveryResult> DiscoverAsync(
        string? savedPath,
        CancellationToken cancellationToken = default) =>
        DiscoverAsync(savedPath, null, cancellationToken);

    public async Task<CodexDiscoveryResult> DiscoverAsync(
        string? customPath,
        string? detectedPath,
        CancellationToken cancellationToken = default)
    {
        var attempted = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(customPath))
        {
            var manual = await ValidateFirstAsync(
                [customPath], CodexDiscoverySource.Manual, attempted, logMissingCandidates: true, cancellationToken).ConfigureAwait(false);
            if (manual.Found) return LogSuccess(manual);
            LogThrottled("manual-path-summary", "[codex-discovery] Manual candidates=1 valid=0; continuing");
        }

        var preferredCandidates = EnumeratePreferredLaunchCandidates().Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        var fromPreferredEntry = await ValidateFirstAsync(
            preferredCandidates, CodexDiscoverySource.PATH, attempted, logMissingCandidates: false, cancellationToken).ConfigureAwait(false);
        if (fromPreferredEntry.Found) return LogSuccess(fromPreferredEntry);

        // An automatically remembered PATH result can point at an older npm
        // wrapper even after the native Codex installation has been updated.
        // Prefer the known installation locations before trusting that cached
        // result; an explicit manual path was already handled above.
        if (!string.IsNullOrWhiteSpace(detectedPath))
        {
            var saved = await ValidateFirstAsync(
                [detectedPath], CodexDiscoverySource.SavedPath, attempted, logMissingCandidates: true, cancellationToken).ConfigureAwait(false);
            if (saved.Found) return LogSuccess(saved);
            LogThrottled("saved-path-summary", "[codex-discovery] SavedPath candidates=1 valid=0; continuing");
        }

        var pathCandidates = EnumeratePathCandidates().Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        LogThrottled("path-summary:" + string.Join("|", pathCandidates), $"[codex-discovery] PATH candidates={pathCandidates.Length}" +
            (pathCandidates.Length == 0 ? "" : $" paths={FormatPaths(pathCandidates)}"));
        var fromPath = await ValidateFirstAsync(
            pathCandidates, CodexDiscoverySource.PATH, attempted, logMissingCandidates: false, cancellationToken).ConfigureAwait(false);
        if (fromPath.Found) return LogSuccess(fromPath);

        var codexProcessScan = ScanProcessPaths(IsCodexProcessName, "Codex");
        var fromCodexProcess = await ValidateFirstAsync(
            EnumerateCodexProcessCandidates(codexProcessScan.Paths),
            CodexDiscoverySource.CodexProcess,
            attempted,
            logMissingCandidates: false,
            cancellationToken).ConfigureAwait(false);
        if (fromCodexProcess.Found) return LogSuccess(fromCodexProcess);

        var chatGPTProcessScan = ScanProcessPaths(IsChatGPTProcessName, "ChatGPT");
        var fromChatGPT = await ValidateFirstAsync(
            EnumerateChatGPTCandidates(chatGPTProcessScan.Paths),
            CodexDiscoverySource.ChatGPTProcess,
            attempted,
            logMissingCandidates: false,
            cancellationToken).ConfigureAwait(false);
        if (fromChatGPT.Found) return LogSuccess(fromChatGPT);

        LogThrottled("discovery-failed", "[codex-discovery] failed sources=SavedPath,PATH,RunningCodex,RunningChatGPT");
        return new CodexDiscoveryResult(false, "", "", CodexDiscoverySource.None);
    }

    public Task<CodexDiscoveryResult> ValidateManualPathAsync(
        string? path,
        CancellationToken cancellationToken = default) =>
        DiscoverCandidatesAsync(path is null ? [] : [path], CodexDiscoverySource.Manual, cancellationToken);

    public async Task<CodexDiscoveryResult> DiscoverCandidatesAsync(
        IEnumerable<string> candidates,
        CodexDiscoverySource source,
        CancellationToken cancellationToken = default)
    {
        var result = await ValidateFirstAsync(
            candidates, source, new HashSet<string>(StringComparer.OrdinalIgnoreCase),
            logMissingCandidates: true, cancellationToken).ConfigureAwait(false);
        return result.Found ? LogSuccess(result) : result;
    }

    private CodexDiscoveryResult LogSuccess(CodexDiscoveryResult result)
    {
        logs.Add("codex-discovery", $"[codex-discovery] discovered path={result.Path} source={result.RuntimeSource}");
        logs.Add("codex-discovery", $"[codex-discovery] validation succeeded path={result.Path} version={result.Version}");
        return result;
    }

    private async Task<CodexDiscoveryResult> ValidateFirstAsync(
        IEnumerable<string> candidates,
        CodexDiscoverySource source,
        HashSet<string> attempted,
        bool logMissingCandidates,
        CancellationToken cancellationToken)
    {
        foreach (var candidate in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var normalized = NormalizeCandidate(candidate, out var normalizationFailure);
            if (normalized is null)
            {
                if (logMissingCandidates)
                    LogThrottled($"candidate-rejected:{source}:{candidate}:{normalizationFailure}",
                        $"[codex-discovery] candidate rejected source={SourceName(source)} path={LogService.Redact(candidate)} reason={normalizationFailure}");
                continue;
            }
            if (!attempted.Add(normalized)) continue;

            logs.Add("codex-discovery", $"[codex-discovery] candidate found source={SourceName(source)} path={normalized}");
            var version = await ValidateCandidateAsync(normalized, source, cancellationToken).ConfigureAwait(false);
            if (version is not null)
                return new CodexDiscoveryResult(true, normalized, version, source);
        }

        return new CodexDiscoveryResult(false, "", "", CodexDiscoverySource.None);
    }

    private static string? NormalizeCandidate(string? candidate, out string failure)
    {
        failure = "";
        if (string.IsNullOrWhiteSpace(candidate))
        {
            failure = "empty";
            return null;
        }
        try
        {
            var expanded = Environment.ExpandEnvironmentVariables(candidate.Trim().Trim('"'));
            var fullPath = Path.GetFullPath(expanded);
            if (IsPackagedAppInternalPath(fullPath))
            {
                failure = "packaged-app-internal-path";
                return null;
            }
            if (File.Exists(fullPath)) return fullPath;
            failure = "file-not-found";
            return null;
        }
        catch (Exception exception) when (exception is ArgumentException or IOException or UnauthorizedAccessException or NotSupportedException)
        {
            failure = exception.GetType().Name;
            return null;
        }
    }

    private async Task<string?> ValidateCandidateAsync(
        string path,
        CodexDiscoverySource source,
        CancellationToken cancellationToken)
    {
        using var process = new Process { StartInfo = CreateVersionStartInfo(path) };
        try
        {
            if (!process.Start())
            {
                LogValidationFailure(path, source, "process-start-returned-false");
                return null;
            }
            var standardOutput = process.StandardOutput.ReadToEndAsync(cancellationToken);
            var standardError = process.StandardError.ReadToEndAsync(cancellationToken);
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(5));
            await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
            var output = await standardOutput.ConfigureAwait(false);
            var error = await standardError.ConfigureAwait(false);
            if (process.ExitCode != 0)
            {
                var detail = FirstNonEmptyLine(error) ?? FirstNonEmptyLine(output) ?? "no-output";
                LogValidationFailure(path, source, $"exit-code-{process.ExitCode} detail={LogService.Redact(detail)}");
                return null;
            }

            var version = FirstNonEmptyLine(output) ?? FirstNonEmptyLine(error);
            if (string.IsNullOrWhiteSpace(version))
            {
                LogValidationFailure(path, source, "empty-version-output");
                return null;
            }
            return LogService.Redact(version.Length <= 128 ? version : version[..128]);
        }
        catch (OperationCanceledException)
        {
            TryKill(process);
            if (cancellationToken.IsCancellationRequested) throw;
            LogValidationFailure(path, source, "validation-timeout");
            return null;
        }
        catch (Exception exception) when (exception is InvalidOperationException or System.ComponentModel.Win32Exception or IOException or UnauthorizedAccessException)
        {
            TryKill(process);
            LogValidationFailure(path, source, $"{exception.GetType().Name}: {LogService.Redact(exception.Message)}");
            return null;
        }
    }

    private void LogValidationFailure(string path, CodexDiscoverySource source, string reason) =>
        LogThrottled($"validation-failed:{source}:{path}:{reason}",
            $"[codex-discovery] validation failed source={SourceName(source)} path={path} reason={reason}");

    private static ProcessStartInfo CreateVersionStartInfo(string path)
    {
        var startInfo = new ProcessStartInfo
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
            WorkingDirectory = Path.GetDirectoryName(path) ?? AppContext.BaseDirectory
        };
        var extension = Path.GetExtension(path);
        if (extension.Equals(".cmd", StringComparison.OrdinalIgnoreCase) ||
            extension.Equals(".bat", StringComparison.OrdinalIgnoreCase))
        {
            startInfo.FileName = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("ComSpec"))
                ? "cmd.exe"
                : Environment.GetEnvironmentVariable("ComSpec")!;
            startInfo.Arguments = $"/d /s /c \"\"{path}\" --version\"";
        }
        else
        {
            startInfo.FileName = path;
            startInfo.ArgumentList.Add("--version");
        }
        return startInfo;
    }

    private static string? FirstNonEmptyLine(string value) =>
        value.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault(line => !string.IsNullOrWhiteSpace(line));

    private static void TryKill(Process process)
    {
        try
        {
            if (!process.HasExited) process.Kill(entireProcessTree: true);
        }
        catch
        {
            // A failed candidate must not stop the remaining discovery methods.
        }
    }

    private static IEnumerable<string> EnumeratePathCandidates()
    {
        var pathValue = Environment.GetEnvironmentVariable("PATH") ?? "";
        var extensions = (Environment.GetEnvironmentVariable("PATHEXT") ?? ".EXE;.CMD;.BAT")
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var rawDirectory in pathValue.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            var directory = rawDirectory.Trim().Trim('"');
            if (directory.Length == 0) continue;
            yield return Path.Combine(directory, "codex");
            foreach (var extension in extensions)
                yield return Path.Combine(directory, "codex" + extension.ToLowerInvariant());
        }
    }

    private static IEnumerable<string> EnumeratePreferredLaunchCandidates()
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localAppData)) yield break;

        // Current Codex desktop builds install the usable app-server binary
        // under a versioned directory here, rather than next to the PATH
        // shim.  The shim may report the same CLI version while lacking the
        // paginated history RPCs exposed by the desktop installation.
        var nativeDirectory = Path.Combine(localAppData, "OpenAI", "Codex", "bin");
        foreach (var candidate in EnumerateInstallationCandidates(nativeDirectory))
            yield return candidate;

        var win32Directory = Path.Combine(localAppData, "Programs", "OpenAI", "Codex", "bin");
        foreach (var name in CodexFileNames)
            yield return Path.Combine(win32Directory, name);

        var aliasDirectory = Path.Combine(localAppData, "Microsoft", "WindowsApps");
        foreach (var name in CodexFileNames)
            yield return Path.Combine(aliasDirectory, name);
    }

    private ProcessScanResult ScanProcessPaths(Func<string, bool> matches, string label)
    {
        Process[] processes;
        try { processes = Process.GetProcesses(); }
        catch (Exception exception)
        {
            logs.Add("codex-discovery", $"[codex-discovery] {label} process enumeration failed type={exception.GetType().Name}");
            return new ProcessScanResult([]);
        }

        var matched = 0;
        var paths = new List<string>();
        foreach (var process in processes)
        {
            using (process)
            {
                string processName;
                try { processName = process.ProcessName; }
                catch { continue; }
                if (!matches(processName)) continue;
                matched++;

                try
                {
                    var executablePath = process.MainModule?.FileName;
                    if (!string.IsNullOrWhiteSpace(executablePath)) paths.Add(executablePath);
                }
                catch (Exception exception)
                {
                    LogThrottled($"main-module:{label}:{process.Id}:{exception.GetType().Name}",
                        $"[codex-discovery] MainModule unavailable process={label} pid={process.Id} type={exception.GetType().Name}");
                }
            }
        }

        var distinctPaths = paths.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        LogThrottled($"process-summary:{label}:{matched}:{string.Join("|", distinctPaths)}",
            $"[codex-discovery] {label} processes={matched} executablePaths={distinctPaths.Length}");
        return new ProcessScanResult(distinctPaths);
    }

    private static bool IsCodexProcessName(string processName) =>
        processName.Equals("codex", StringComparison.OrdinalIgnoreCase) ||
        processName.StartsWith("codex-", StringComparison.OrdinalIgnoreCase);

    private static bool IsChatGPTProcessName(string processName) =>
        processName.Equals("ChatGPT", StringComparison.OrdinalIgnoreCase);

    private IEnumerable<string> EnumerateCodexProcessCandidates(IEnumerable<string> processPaths)
    {
        foreach (var processPath in processPaths)
        {
            if (IsPackagedAppInternalPath(processPath))
            {
                LogThrottled($"package-clue:Codex:{processPath}",
                    $"[codex-discovery] RunningCodex package path retained as installation clue only path={LogService.Redact(processPath)}");
                foreach (var launchEntry in EnumeratePreferredLaunchCandidates()) yield return launchEntry;
                continue;
            }
            var fileName = Path.GetFileNameWithoutExtension(processPath);
            if (fileName.Equals("codex", StringComparison.OrdinalIgnoreCase)) yield return processPath;
            var directory = Path.GetDirectoryName(processPath);
            if (directory is null) continue;
            foreach (var candidateName in CodexFileNames) yield return Path.Combine(directory, candidateName);
        }
    }

    private IEnumerable<string> EnumerateChatGPTCandidates(IEnumerable<string> chatGPTPaths)
    {
        foreach (var chatGPTPath in chatGPTPaths.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            if (IsPackagedAppInternalPath(chatGPTPath))
            {
                LogThrottled($"package-clue:ChatGPT:{chatGPTPath}",
                    $"[codex-discovery] RunningChatGPT package path retained as installation clue only path={LogService.Redact(chatGPTPath)}");
                foreach (var launchEntry in EnumeratePreferredLaunchCandidates()) yield return launchEntry;
                continue;
            }
            var installationDirectory = Path.GetDirectoryName(chatGPTPath);
            if (installationDirectory is null) continue;
            LogThrottled($"chatgpt-directory:{installationDirectory}",
                $"[codex-discovery] scanning ChatGPT installation directory={installationDirectory}");
            foreach (var candidate in EnumerateInstallationCandidates(installationDirectory)) yield return candidate;
        }

        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (!string.IsNullOrWhiteSpace(localAppData))
        {
            var fallbackDirectory = Path.Combine(localAppData, "Programs", "OpenAI", "Codex", "bin");
            LogThrottled($"chatgpt-fallback:{fallbackDirectory}",
                $"[codex-discovery] scanning ChatGPT fallback directory={fallbackDirectory}");
            foreach (var name in CodexFileNames)
                yield return Path.Combine(fallbackDirectory, name);
        }
    }

    public static IEnumerable<string> EnumerateInstallationCandidates(string installationDirectory)
    {
        foreach (var relativeDirectory in CommonChatGPTRelativeDirectories)
            foreach (var fileName in CodexFileNames)
                yield return Path.Combine(installationDirectory, relativeDirectory, fileName);

        var queue = new Queue<(string Directory, int Depth)>();
        queue.Enqueue((installationDirectory, 0));
        var searchedDirectories = 0;
        while (queue.Count > 0 && searchedDirectories < MaximumSearchedDirectories)
        {
            var (directory, depth) = queue.Dequeue();
            searchedDirectories++;
            string[] entries;
            try { entries = Directory.EnumerateFileSystemEntries(directory).ToArray(); }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or System.Security.SecurityException)
            {
                continue;
            }

            foreach (var entry in entries)
            {
                string fileName;
                try { fileName = Path.GetFileName(entry); }
                catch { continue; }
                if (CodexFileNames.Contains(fileName, StringComparer.OrdinalIgnoreCase)) yield return entry;
            }
            if (depth >= MaximumSearchDepth) continue;

            foreach (var entry in entries)
            {
                try
                {
                    var attributes = File.GetAttributes(entry);
                    if (!attributes.HasFlag(FileAttributes.Directory) || attributes.HasFlag(FileAttributes.ReparsePoint)) continue;
                    queue.Enqueue((entry, depth + 1));
                }
                catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or System.Security.SecurityException)
                {
                    // WindowsApps/MSIX entries may disappear or deny access during enumeration.
                }
            }
        }
    }

    public static bool IsPackagedAppInternalPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path)) return false;
        try
        {
            var normalized = Path.GetFullPath(Environment.ExpandEnvironmentVariables(path.Trim().Trim('"')))
                .Replace(Path.AltDirectorySeparatorChar, Path.DirectorySeparatorChar);
            var programFilesRoots = new[]
            {
                Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles),
                Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86),
                Environment.GetEnvironmentVariable("ProgramW6432") ?? ""
            };
            return programFilesRoots
                .Where(root => !string.IsNullOrWhiteSpace(root))
                .Select(root => Path.Combine(Path.GetFullPath(root), "WindowsApps") + Path.DirectorySeparatorChar)
                .Any(root => normalized.StartsWith(root, StringComparison.OrdinalIgnoreCase));
        }
        catch (Exception exception) when (exception is ArgumentException or IOException or NotSupportedException)
        {
            return false;
        }
    }

    private void LogThrottled(string key, string message)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var candidate in _recentDiagnostics)
            if (now - candidate.Value >= TimeSpan.FromMinutes(10)) _recentDiagnostics.TryRemove(candidate.Key, out _);
        if (_recentDiagnostics.Count >= MaximumRecentDiagnostics && !_recentDiagnostics.ContainsKey(key))
        {
            var oldest = _recentDiagnostics.OrderBy(pair => pair.Value).FirstOrDefault();
            if (!string.IsNullOrEmpty(oldest.Key)) _recentDiagnostics.TryRemove(oldest.Key, out _);
        }
        if (_recentDiagnostics.TryGetValue(key, out var previous) && now - previous < TimeSpan.FromSeconds(30)) return;
        _recentDiagnostics[key] = now;
        logs.Add("codex-discovery", message);
    }

    private static string SourceName(CodexDiscoverySource source) => source switch
    {
        CodexDiscoverySource.CodexProcess => "RunningCodex",
        CodexDiscoverySource.ChatGPTProcess => "RunningChatGPT",
        CodexDiscoverySource.Manual => "Manual",
        _ => source.ToString()
    };

    private static string FormatPaths(IEnumerable<string> paths) =>
        string.Join(";", paths.Select(LogService.Redact));

    private sealed record ProcessScanResult(IReadOnlyList<string> Paths);
}
