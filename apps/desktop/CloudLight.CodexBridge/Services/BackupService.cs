using System.Diagnostics;
using System.IO.Compression;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CloudLight.CodexBridge.Models;
using Microsoft.Data.Sqlite;

namespace CloudLight.CodexBridge.Services;

public sealed class BackupService
{
    private const int CurrentFormatVersion = 2;
    private const int OldestSupportedFormatVersion = 1;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = true
    };
    private static readonly HashSet<string> RuntimeDirectoryNames = new(StringComparer.OrdinalIgnoreCase)
    {
        ".sandbox", ".sandbox-bin", ".sandbox-secrets", ".tmp", "cache", "caches", "logs", "log",
        "temp", "tmp", "runtime", "thread-writer-locks", "node_repl", "process_manager"
    };
    private static readonly HashSet<string> RuntimeExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".log", ".tmp", ".lock", ".pid", ".sock"
    };

    private readonly SettingsService _settings;
    private readonly LogService? _logs;
    private readonly string? _codexHomeOverride;
    private readonly string? _bridgeLocalOverride;
    private readonly string? _bridgeRoamingOverride;

    public BackupService(
        SettingsService settings,
        string? codexHome = null,
        string? bridgeLocal = null,
        string? bridgeRoaming = null,
        LogService? logs = null)
    {
        _settings = settings;
        _logs = logs;
        _codexHomeOverride = codexHome;
        _bridgeLocalOverride = bridgeLocal;
        _bridgeRoamingOverride = bridgeRoaming;
    }

    public string CodexHome => _codexHomeOverride is null ? DetectCodexHome() : Path.GetFullPath(_codexHomeOverride);
    public string BridgeLocalData => _bridgeLocalOverride is null ? _settings.DataDirectory : Path.GetFullPath(_bridgeLocalOverride);
    public string BridgeRoamingData => _bridgeRoamingOverride is null ? Path.GetDirectoryName(_settings.SettingsFile)! : Path.GetFullPath(_bridgeRoamingOverride);

    public static string DetectCodexHome()
    {
        var configured = Environment.GetEnvironmentVariable("CODEX_HOME");
        if (!string.IsNullOrWhiteSpace(configured)) return Path.GetFullPath(Environment.ExpandEnvironmentVariables(configured.Trim()));
        return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".codex");
    }

    public async Task<(int Files, long Size)> ScanAsync(bool includeCodex, bool includeBridge, CancellationToken cancellationToken = default)
    {
        var scan = await Task.Run(() => EnumerateFiles(GetSources(includeCodex, includeBridge), cancellationToken), cancellationToken);
        return (scan.Files.Count, scan.Files.Sum(item => item.Size));
    }

    public async Task<BackupResult> CreateBackupAsync(
        string destination,
        bool includeCodex,
        bool includeBridge,
        IProgress<BackupProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        if (!includeCodex && !includeBridge) throw new InvalidOperationException("请至少选择一项备份内容。");
        destination = Path.GetFullPath(destination);
        var sources = GetSources(includeCodex, includeBridge);
        foreach (var source in sources.Where(source => Directory.Exists(source.Root)))
            if (IsWithin(destination, source.Root)) throw new InvalidOperationException("备份文件不能保存在被备份的数据目录内。");

        progress?.Report(new BackupProgress("正在扫描持久化数据", 0, 0, 0, 0));
        var scan = await Task.Run(() => EnumerateFiles(sources, cancellationToken), cancellationToken);
        var manifest = NewManifest(includeCodex, includeBridge);
        manifest.ExcludedRuntimeFiles.AddRange(scan.ExcludedRuntimeFiles);
        manifest.Failures.AddRange(scan.Failures);
        manifest.CriticalFiles.AddRange(scan.Files.Where(file => file.Classification.IsCritical).Select(file => file.ArchivePath));
        manifest.OptionalFiles.AddRange(scan.Files.Where(file => !file.Classification.IsCritical).Select(file => file.ArchivePath));
        foreach (var failure in scan.Failures) LogFailure(failure);

        var totalBytes = scan.Files.Sum(item => item.Size);
        var temporary = destination + $".tmp-{Guid.NewGuid():N}";
        Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
        try
        {
            await using (var output = new FileStream(temporary, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 131072, true))
            using (var archive = new ZipArchive(output, ZipArchiveMode.Create, leaveOpen: false))
            {
                var processedFiles = 0;
                long processedBytes = 0;
                foreach (var file in scan.Files)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    progress?.Report(new BackupProgress("正在校验并压缩", processedFiles, scan.Files.Count, processedBytes, totalBytes));
                    try
                    {
                        await using var capture = await OpenCaptureAsync(file, cancellationToken);
                        var entry = archive.CreateEntry(file.ArchivePath, CompressionLevel.Optimal);
                        entry.LastWriteTime = ClampZipTime(file.LastWriteTime);
                        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                        await using var entryStream = entry.Open();
                        var buffer = new byte[131072];
                        long fileBytes = 0;
                        int read;
                        while ((read = await capture.Stream.ReadAsync(buffer, cancellationToken)) > 0)
                        {
                            await entryStream.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
                            hash.AppendData(buffer, 0, read);
                            processedBytes += read;
                            fileBytes += read;
                            progress?.Report(new BackupProgress("正在压缩", processedFiles, scan.Files.Count, processedBytes, totalBytes));
                        }
                        manifest.Files.Add(new BackupFileRecord
                        {
                            RelativePath = file.ArchivePath,
                            Size = fileBytes,
                            Sha256 = Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant(),
                            LastWriteTime = file.LastWriteTime,
                            Category = file.Classification.Category,
                            Module = file.Classification.Module,
                            IsCritical = file.Classification.IsCritical
                        });
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException)
                    {
                        var failure = CreateFailure(file.FullPath, file.ArchivePath, file.Classification, exception);
                        manifest.Failures.Add(failure);
                        LogFailure(failure);
                    }
                    processedFiles++;
                }

                FinalizeManifest(manifest, manifest.Files.Select(file => NormalizeArchivePath(file.RelativePath)).ToHashSet(StringComparer.OrdinalIgnoreCase));
                manifest.FileCount = manifest.Files.Count;
                manifest.TotalSize = manifest.Files.Sum(item => item.Size);
                var manifestEntry = archive.CreateEntry("manifest.json", CompressionLevel.Optimal);
                await using var manifestStream = manifestEntry.Open();
                await JsonSerializer.SerializeAsync(manifestStream, manifest, JsonOptions, cancellationToken);
            }

            if (!manifest.CanRestore)
                throw new BackupCreationException("没有成功保存任何可识别的持久化数据，未生成可用备份。", manifest);

            File.Move(temporary, destination, overwrite: true);
            var stage = manifest.Status == BackupStatuses.Complete ? "备份完成" : "备份完成（有警告）";
            progress?.Report(new BackupProgress(stage, manifest.FileCount, scan.Files.Count, manifest.TotalSize, totalBytes));
            return new BackupResult { FilePath = destination, Manifest = manifest };
        }
        catch
        {
            TryDeleteFile(temporary);
            throw;
        }
    }

    public async Task<BackupManifest> ReadAndValidateAsync(
        string backupPath,
        IProgress<BackupProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        backupPath = Path.GetFullPath(backupPath);
        try
        {
            await using var input = new FileStream(backupPath, FileMode.Open, FileAccess.Read, FileShare.Read, 131072, true);
            using var archive = new ZipArchive(input, ZipArchiveMode.Read, leaveOpen: false);
            var manifestEntry = archive.Entries.FirstOrDefault(entry =>
                entry.FullName.Equals("manifest.json", StringComparison.OrdinalIgnoreCase));
            BackupManifest manifest;
            var inferredManifest = manifestEntry is null;
            if (inferredManifest)
            {
                manifest = InferManifestFromArchive(archive, backupPath);
            }
            else
            {
                try
                {
                    await using var stream = manifestEntry!.Open();
                    using var buffer = new MemoryStream();
                    await stream.CopyToAsync(buffer, cancellationToken);
                    var manifestBytes = buffer.ToArray();
                    manifest = JsonSerializer.Deserialize<BackupManifest>(manifestBytes, JsonOptions)
                        ?? throw new InvalidDataException("备份无法识别：manifest 为空。");
                    MergeLegacyFailures(manifest, manifestBytes);
                }
                catch (JsonException exception)
                {
                    throw new InvalidDataException("备份无法识别：manifest 格式严重损坏。", exception);
                }
            }

            if (!inferredManifest && (manifest.FormatVersion < OldestSupportedFormatVersion || manifest.FormatVersion > CurrentFormatVersion))
                throw new InvalidDataException($"不支持的备份格式版本：{manifest.FormatVersion}。");
            NormalizeManifest(manifest);
            if (inferredManifest)
                AddValidationIssue(manifest, "manifest.json", "", "manifest 缺失；已根据安全路径识别可恢复数据。");

            var archiveEntries = new Dictionary<string, ZipArchiveEntry>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in archive.Entries.Where(entry => !string.IsNullOrEmpty(entry.Name) &&
                                                                  !entry.FullName.Equals("manifest.json", StringComparison.OrdinalIgnoreCase)))
            {
                var path = NormalizeArchivePath(entry.FullName);
                if (!IsAllowedArchivePath(path))
                    throw new InvalidDataException($"备份容器包含不安全路径：{entry.FullName}");
                if (!archiveEntries.TryAdd(path, entry))
                    throw new InvalidDataException($"备份容器包含重复路径：{path}");
            }

            var records = new Dictionary<string, BackupFileRecord>(StringComparer.OrdinalIgnoreCase);
            foreach (var record in manifest.Files)
            {
                var path = NormalizeArchivePath(record.RelativePath);
                if (!IsAllowedArchivePath(path))
                    throw new InvalidDataException($"manifest 包含不安全路径：{record.RelativePath}");
                if (!records.TryAdd(path, record))
                    throw new InvalidDataException($"manifest 包含重复路径：{path}");
                record.RelativePath = path;
                ApplyClassification(record);
            }
            foreach (var failure in manifest.Failures) ApplyClassification(failure);

            if (manifest.FileCount != manifest.Files.Count)
                AddValidationIssue(manifest, "manifest.json", "", $"文件计数不一致：记录 {manifest.FileCount}，实际 {manifest.Files.Count}。 ");
            if (manifest.TotalSize != manifest.Files.Sum(file => file.Size))
                AddValidationIssue(manifest, "manifest.json", "", "总大小与文件记录不一致。");

            var validPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            long processed = 0;
            var index = 0;
            foreach (var (path, record) in records)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (IsExcludedRuntimePath(path))
                {
                    if (!manifest.ExcludedRuntimeFiles.Contains(path, StringComparer.OrdinalIgnoreCase)) manifest.ExcludedRuntimeFiles.Add(path);
                    continue;
                }
                if (!archiveEntries.TryGetValue(path, out var entry))
                {
                    AddValidationIssue(manifest, path, record.Module, "归档中缺少该文件。");
                    continue;
                }

                progress?.Report(new BackupProgress("正在验证可恢复数据", index, manifest.Files.Count, processed, manifest.TotalSize));
                try
                {
                    if (entry.Length != record.Size) throw new InvalidDataException("文件大小不一致。");
                    await using var stream = entry.Open();
                    var actualHash = Convert.ToHexString(await SHA256.HashDataAsync(stream, cancellationToken)).ToLowerInvariant();
                    if (inferredManifest) record.Sha256 = actualHash;
                    else if (!IsValidSha256(record.Sha256) || !actualHash.Equals(record.Sha256, StringComparison.OrdinalIgnoreCase))
                        throw new InvalidDataException("SHA-256 不一致。");
                    await ValidateKnownContentAsync(entry, path, cancellationToken);
                    validPaths.Add(path);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    AddValidationIssue(manifest, path, record.Module, $"{exception.GetType().Name}: {exception.Message}");
                }
                processed += Math.Max(0, record.Size);
                index++;
            }

            foreach (var extra in archiveEntries.Keys.Where(path => !records.ContainsKey(path)))
            {
                if (MatchesRecordedFailure(extra, manifest.Failures) || IsExcludedRuntimePath(extra)) continue;
                AddValidationIssue(manifest, extra, Classify(extra).Module, "归档中存在未被 manifest 记录的文件，已忽略。");
            }

            FinalizeManifest(manifest, validPaths);
            if (!manifest.CanRestore)
                throw new InvalidDataException("备份中没有任何能够安全识别和读取的持久化数据。");
            return manifest;
        }
        catch (InvalidDataException)
        {
            throw;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            throw new InvalidDataException("备份文件或 ZIP 容器无法读取。", exception);
        }
    }

    public async Task<RestoreResult> RestoreAsync(
        string backupPath,
        RestoreOptions options,
        Func<Task>? stopRuntime,
        Func<Task>? restartRuntime,
        IProgress<BackupProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        if (!options.RestoreCodex && !options.RestoreBridge) throw new InvalidOperationException("请至少选择一项恢复内容。");
        var manifest = await ReadAndValidateAsync(backupPath, progress, cancellationToken);
        if (options.RestoreCodex && !manifest.IncludedCodex) throw new InvalidOperationException("此备份不包含 Codex 数据。");
        if (options.RestoreBridge && !manifest.IncludedBridge) throw new InvalidOperationException("此备份不包含 Bridge 数据。");

        var selectedRecords = manifest.Files.Where(file => IsSelected(file.RelativePath, options) &&
                                                           !IsExcludedRuntimePath(file.RelativePath) &&
                                                           !manifest.ValidationIssues.Any(issue => issue.RelativePath.Equals(file.RelativePath, StringComparison.OrdinalIgnoreCase)))
                                            .ToList();
        if (selectedRecords.Count == 0) throw new InvalidDataException("所选范围内没有可安全恢复的数据。");

        var warnings = manifest.Failures.Where(failure => IsSelected(failure.RelativePath, options))
            .Select(failure => new BackupRestoreWarning
            {
                RelativePath = failure.RelativePath,
                Module = failure.Module,
                Error = $"创建备份时未保存：{failure.ExceptionType}: {failure.Error}".Trim(),
                AffectsOtherModules = false
            }).Concat(manifest.ValidationIssues.Where(issue => IsSelected(issue.RelativePath, options)).Select(issue => new BackupRestoreWarning
            {
                RelativePath = issue.RelativePath,
                Module = issue.Module,
                Error = issue.Error,
                AffectsOtherModules = false
            })).ToList();

        var tempRoot = Path.Combine(Path.GetTempPath(), $"CloudLight-CodexBridge-Restore-{Guid.NewGuid():N}");
        var stagingRoot = Path.Combine(tempRoot, "staging");
        Directory.CreateDirectory(stagingRoot);
        var extracted = new List<(BackupFileRecord Record, string StagingPath)>();
        var runtimeStopped = false;
        var preRestore = "";
        try
        {
            progress?.Report(new BackupProgress("正在解压并分析恢复模块", 0, selectedRecords.Count, 0, selectedRecords.Sum(file => file.Size)));
            await ExtractRecoverableAsync(backupPath, selectedRecords, stagingRoot, extracted, warnings, progress, cancellationToken);
            if (extracted.Count == 0) throw new InvalidDataException("所选范围内的文件均无法安全解压或校验。");

            Directory.CreateDirectory(options.PreRestoreDirectory);
            preRestore = Path.Combine(options.PreRestoreDirectory, $"PreRestore-{DateTime.Now:yyyyMMdd-HHmmss}.clcbak");
            var preResult = await CreateBackupAsync(preRestore, options.RestoreCodex, options.RestoreBridge, progress, cancellationToken);
            foreach (var failure in preResult.Manifest.Failures)
                warnings.Add(new BackupRestoreWarning
                {
                    RelativePath = failure.RelativePath,
                    Module = failure.Module,
                    Error = $"恢复前备份警告：{failure.ExceptionType}: {failure.Error}",
                    AffectsOtherModules = false
                });

            if (stopRuntime is not null && restartRuntime is null)
                throw new InvalidOperationException("暂停运行服务后必须提供对应的重新初始化操作。");
            if (stopRuntime is not null)
            {
                runtimeStopped = true;
                await stopRuntime();
            }
            var conflicts = options.VerifyNoExternalCodex && options.RestoreCodex ? GetExternalCodexProcesses() : [];
            if (conflicts.Count > 0) throw new InvalidOperationException($"检测到外部 Codex 正在运行，请关闭后重试：{string.Join("、", conflicts)}");

            var succeededModules = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var failedModules = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var restoredFiles = 0;
            foreach (var moduleGroup in extracted.GroupBy(item => item.Record.Module, StringComparer.OrdinalIgnoreCase))
            {
                var moduleRestored = false;
                foreach (var item in moduleGroup)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    try
                    {
                        var target = GetRestoreTarget(item.Record.RelativePath);
                        if (target is null) continue;
                        Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                        if (!options.Replace && File.Exists(target))
                        {
                            moduleRestored = true;
                            continue;
                        }
                        var incoming = target + $".restore-new-{Guid.NewGuid():N}";
                        try
                        {
                            File.Copy(item.StagingPath, incoming, overwrite: false);
                            File.Move(incoming, target, overwrite: true);
                        }
                        finally
                        {
                            TryDeleteFile(incoming);
                        }
                        File.SetLastWriteTimeUtc(target, item.Record.LastWriteTime.UtcDateTime);
                        restoredFiles++;
                        moduleRestored = true;
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException)
                    {
                        warnings.Add(new BackupRestoreWarning
                        {
                            RelativePath = item.Record.RelativePath,
                            Module = item.Record.Module,
                            Error = $"{exception.GetType().Name}: {exception.Message}",
                            AffectsOtherModules = false
                        });
                    }
                }
                if (moduleRestored) succeededModules.Add(moduleGroup.Key);
                else failedModules.Add(moduleGroup.Key);
            }

            foreach (var module in manifest.Modules.Where(module => IsModuleSelected(module.Module, options) && !module.CanRestore))
                failedModules.Add(module.Module);

            if (succeededModules.Count == 0) throw new IOException("没有任何数据模块恢复成功。");

            if (restartRuntime is not null)
            {
                try
                {
                    await restartRuntime();
                }
                catch (Exception exception)
                {
                    warnings.Add(new BackupRestoreWarning
                    {
                        Module = "runtime-reload",
                        Error = $"数据已恢复，但运行服务重新初始化失败：{exception.GetType().Name}: {exception.Message}",
                        AffectsOtherModules = true
                    });
                }
                finally
                {
                    runtimeStopped = false;
                }
            }

            return new RestoreResult
            {
                Manifest = manifest,
                PreRestoreBackupPath = preRestore,
                RestoredFiles = restoredFiles,
                SucceededModules = succeededModules.OrderBy(GetModuleSortOrder).ToList(),
                FailedModules = failedModules.OrderBy(GetModuleSortOrder).ToList(),
                Warnings = warnings
            };
        }
        finally
        {
            if (runtimeStopped && restartRuntime is not null)
            {
                try { await restartRuntime(); }
                catch (Exception exception) { _logs?.AddException("backup", "恢复中断后重新初始化运行服务失败。", exception); }
            }
            TryDeleteDirectory(tempRoot);
        }
    }

    public static IReadOnlyList<string> GetExternalCodexProcesses()
    {
        var result = new List<string>();
        foreach (var process in Process.GetProcesses())
        {
            try
            {
                var name = process.ProcessName;
                if (name.Contains("codex", StringComparison.OrdinalIgnoreCase) &&
                    !name.Contains("CodexBridge", StringComparison.OrdinalIgnoreCase)) result.Add($"{name} (PID {process.Id})");
            }
            catch { }
            finally { process.Dispose(); }
        }
        return result;
    }

    public static string GetModuleDisplayName(string module) => module switch
    {
        BackupModules.ApplicationSettings => "应用设置",
        BackupModules.CodexSettings => "Codex 设置",
        BackupModules.Qq => "QQ 配置",
        BackupModules.Telegram => "Telegram 配置",
        BackupModules.Bindings => "会话绑定",
        BackupModules.Commands => "指令配置",
        BackupModules.MessageSync => "消息同步配置",
        BackupModules.ThreadState => "会话编号状态",
        BackupModules.Sessions => "Codex 会话与历史",
        BackupModules.OtherPersistentData => "其他持久化数据",
        BackupModules.RuntimeExcluded => "运行时文件（已跳过）",
        "runtime-reload" => "运行服务重载",
        _ => string.IsNullOrWhiteSpace(module) ? "未知模块" : module
    };

    private BackupManifest NewManifest(bool includeCodex, bool includeBridge) => new()
    {
        FormatVersion = CurrentFormatVersion,
        CreatedAt = DateTimeOffset.Now,
        AppVersion = Assembly.GetExecutingAssembly().GetName().Version?.ToString(3) ?? "1.1.3",
        CodexVersion = TryGetCodexVersion(),
        MachineName = Environment.MachineName,
        CodexHome = CodexHome,
        IncludedCodex = includeCodex,
        IncludedBridge = includeBridge
    };

    private List<BackupSource> GetSources(bool includeCodex, bool includeBridge)
    {
        var result = new List<BackupSource>();
        if (includeCodex) result.Add(new BackupSource(CodexHome, "codex"));
        if (includeBridge)
        {
            result.Add(new BackupSource(BridgeLocalData, "bridge/local"));
            if (!Path.GetFullPath(BridgeRoamingData).Equals(Path.GetFullPath(BridgeLocalData), StringComparison.OrdinalIgnoreCase))
                result.Add(new BackupSource(BridgeRoamingData, "bridge/roaming"));
        }
        return result;
    }

    private static ScanResult EnumerateFiles(IEnumerable<BackupSource> sources, CancellationToken cancellationToken)
    {
        var result = new ScanResult();
        foreach (var source in sources)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!Directory.Exists(source.Root)) continue;
            var pending = new Stack<string>();
            pending.Push(source.Root);
            while (pending.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var directory = pending.Pop();
                try
                {
                    foreach (var child in Directory.GetDirectories(directory))
                    {
                        var relativeDirectory = Path.GetRelativePath(source.Root, child).Replace('\\', '/');
                        var archiveDirectory = $"{source.Prefix}/{relativeDirectory}";
                        if (IsExcludedRuntimeDirectory(archiveDirectory)) result.ExcludedRuntimeFiles.Add(archiveDirectory + "/**");
                        else pending.Push(child);
                    }
                    foreach (var path in Directory.GetFiles(directory))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var archivePath = $"{source.Prefix}/{Path.GetRelativePath(source.Root, path).Replace('\\', '/')}";
                        var classification = Classify(archivePath);
                        if (classification.Excluded)
                        {
                            result.ExcludedRuntimeFiles.Add(archivePath);
                            continue;
                        }
                        try
                        {
                            var info = new FileInfo(path);
                            result.Files.Add(new SourceFile(path, archivePath, info.Length, info.LastWriteTimeUtc, classification));
                        }
                        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
                        {
                            result.Failures.Add(CreateFailure(path, archivePath, classification, exception));
                        }
                    }
                }
                catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
                {
                    var relative = Path.GetRelativePath(source.Root, directory).Replace('\\', '/');
                    var archivePath = $"{source.Prefix}/{relative}/**";
                    result.Failures.Add(CreateFailure(directory, archivePath, Classify(archivePath), exception));
                }
            }
        }
        return result;
    }

    private static async Task ExtractRecoverableAsync(
        string backupPath,
        IReadOnlyList<BackupFileRecord> records,
        string stagingRoot,
        List<(BackupFileRecord Record, string StagingPath)> extracted,
        List<BackupRestoreWarning> warnings,
        IProgress<BackupProgress>? progress,
        CancellationToken cancellationToken)
    {
        await using var input = new FileStream(backupPath, FileMode.Open, FileAccess.Read, FileShare.Read, 131072, true);
        using var archive = new ZipArchive(input, ZipArchiveMode.Read);
        var entries = archive.Entries.Where(entry => !string.IsNullOrEmpty(entry.Name))
            .ToDictionary(entry => NormalizeArchivePath(entry.FullName), StringComparer.OrdinalIgnoreCase);
        long bytes = 0;
        var totalBytes = records.Sum(file => file.Size);
        for (var index = 0; index < records.Count; index++)
        {
            var record = records[index];
            var path = NormalizeArchivePath(record.RelativePath);
            try
            {
                var target = Path.GetFullPath(Path.Combine(stagingRoot, path.Replace('/', Path.DirectorySeparatorChar)));
                if (!IsWithin(target, stagingRoot)) throw new InvalidDataException("路径超出临时恢复目录。");
                Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                await using (var source = entries[path].Open())
                await using (var destination = new FileStream(target, FileMode.CreateNew, FileAccess.Write, FileShare.None, 131072, true))
                {
                    await source.CopyToAsync(destination, cancellationToken);
                    await destination.FlushAsync(cancellationToken);
                }
                await ValidateKnownFileAsync(target, path, cancellationToken);
                File.SetLastWriteTimeUtc(target, record.LastWriteTime.UtcDateTime);
                extracted.Add((record, target));
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                warnings.Add(new BackupRestoreWarning
                {
                    RelativePath = path,
                    Module = record.Module,
                    Error = $"解压或内容校验失败：{exception.GetType().Name}: {exception.Message}",
                    AffectsOtherModules = false
                });
            }
            bytes += record.Size;
            progress?.Report(new BackupProgress("正在解压并分析恢复模块", index + 1, records.Count, bytes, totalBytes));
        }
    }

    private string? GetRestoreTarget(string archivePath)
    {
        var normalized = NormalizeArchivePath(archivePath);
        if (normalized.StartsWith("codex/", StringComparison.OrdinalIgnoreCase))
            return SafeTarget(CodexHome, normalized["codex/".Length..]);
        if (normalized.StartsWith("bridge/local/", StringComparison.OrdinalIgnoreCase))
            return SafeTarget(BridgeLocalData, normalized["bridge/local/".Length..]);
        if (normalized.StartsWith("bridge/roaming/", StringComparison.OrdinalIgnoreCase))
            return SafeTarget(BridgeRoamingData, normalized["bridge/roaming/".Length..]);
        return null;
    }

    private static string SafeTarget(string root, string relative)
    {
        var target = Path.GetFullPath(Path.Combine(root, relative.Replace('/', Path.DirectorySeparatorChar)));
        if (!IsWithin(target, root)) throw new InvalidDataException($"恢复路径超出数据目录：{relative}");
        return target;
    }

    private static async Task<FileCapture> OpenCaptureAsync(SourceFile file, CancellationToken cancellationToken)
    {
        if (await IsSqliteDatabaseAsync(file.FullPath, cancellationToken))
        {
            var snapshot = Path.Combine(Path.GetTempPath(), $"CloudLight-CodexBridge-Sqlite-{Guid.NewGuid():N}.db");
            try
            {
                await Task.Run(() =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var sourceBuilder = new SqliteConnectionStringBuilder { DataSource = file.FullPath, Mode = SqliteOpenMode.ReadOnly, Cache = SqliteCacheMode.Private };
                    var targetBuilder = new SqliteConnectionStringBuilder { DataSource = snapshot, Mode = SqliteOpenMode.ReadWriteCreate, Cache = SqliteCacheMode.Private };
                    using var source = new SqliteConnection(sourceBuilder.ToString());
                    using var target = new SqliteConnection(targetBuilder.ToString());
                    source.Open();
                    target.Open();
                    source.BackupDatabase(target);
                    using var command = target.CreateCommand();
                    command.CommandText = "PRAGMA quick_check";
                    var result = command.ExecuteScalar()?.ToString();
                    if (!string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase))
                        throw new InvalidDataException($"SQLite 快照校验失败：{result}");
                }, cancellationToken);
                return new FileCapture(new FileStream(snapshot, FileMode.Open, FileAccess.Read, FileShare.Read, 131072, true), snapshot);
            }
            catch
            {
                TryDeleteFile(snapshot);
                throw;
            }
        }
        return new FileCapture(await OpenReadWithRetryAsync(file.FullPath, cancellationToken), null);
    }

    private static async Task<bool> IsSqliteDatabaseAsync(string path, CancellationToken cancellationToken)
    {
        var extension = Path.GetExtension(path);
        if (!extension.Equals(".sqlite", StringComparison.OrdinalIgnoreCase) && !extension.Equals(".db", StringComparison.OrdinalIgnoreCase)) return false;
        await using var stream = await OpenReadWithRetryAsync(path, cancellationToken);
        if (stream.Length < 16) return false;
        var header = new byte[16];
        var read = await stream.ReadAsync(header, cancellationToken);
        return read == 16 && Encoding.ASCII.GetString(header) == "SQLite format 3\0";
    }

    private static async Task ValidateKnownContentAsync(ZipArchiveEntry entry, string path, CancellationToken cancellationToken)
    {
        if (!RequiresJsonValidation(path) && !path.Equals("codex/config.toml", StringComparison.OrdinalIgnoreCase)) return;
        await using var stream = entry.Open();
        if (RequiresJsonValidation(path))
        {
            using var _ = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
            return;
        }
        using var reader = new StreamReader(stream, new UTF8Encoding(false, true), detectEncodingFromByteOrderMarks: true, leaveOpen: false);
        _ = await reader.ReadToEndAsync(cancellationToken);
    }

    private static async Task ValidateKnownFileAsync(string filePath, string archivePath, CancellationToken cancellationToken)
    {
        if (RequiresJsonValidation(archivePath))
        {
            await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 131072, true);
            using var _ = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
            return;
        }
        if (archivePath.Equals("codex/config.toml", StringComparison.OrdinalIgnoreCase))
        {
            using var reader = new StreamReader(filePath, new UTF8Encoding(false, true), detectEncodingFromByteOrderMarks: true);
            _ = await reader.ReadToEndAsync(cancellationToken);
        }
    }

    private static bool RequiresJsonValidation(string path)
    {
        path = NormalizeArchivePath(path);
        return path.Equals("bridge/roaming/settings.json", StringComparison.OrdinalIgnoreCase) ||
               path.Equals("bridge/local/bindings.json", StringComparison.OrdinalIgnoreCase) ||
               path.StartsWith("bridge/local/data/", StringComparison.OrdinalIgnoreCase) && path.EndsWith(".json", StringComparison.OrdinalIgnoreCase) ||
               path.Equals("codex/auth.json", StringComparison.OrdinalIgnoreCase) ||
               path.Equals("codex/.codex-global-state.json", StringComparison.OrdinalIgnoreCase);
    }

    private static BackupManifest InferManifestFromArchive(ZipArchive archive, string backupPath)
    {
        var manifest = new BackupManifest
        {
            FormatVersion = 1,
            CreatedAt = File.GetLastWriteTimeUtc(backupPath),
            AppVersion = "未知（无 manifest）"
        };
        foreach (var entry in archive.Entries.Where(entry => !string.IsNullOrEmpty(entry.Name)))
        {
            var path = NormalizeArchivePath(entry.FullName);
            if (!IsAllowedArchivePath(path))
                throw new InvalidDataException($"无 manifest 的备份包含不安全路径：{entry.FullName}");
            var classification = Classify(path);
            manifest.Files.Add(new BackupFileRecord
            {
                RelativePath = path,
                Size = entry.Length,
                LastWriteTime = entry.LastWriteTime,
                Category = classification.Category,
                Module = classification.Module,
                IsCritical = classification.IsCritical
            });
            if (classification.IsCritical) manifest.CriticalFiles.Add(path);
            else manifest.OptionalFiles.Add(path);
            manifest.IncludedCodex |= path.StartsWith("codex/", StringComparison.OrdinalIgnoreCase);
            manifest.IncludedBridge |= path.StartsWith("bridge/", StringComparison.OrdinalIgnoreCase);
        }
        manifest.FileCount = manifest.Files.Count;
        manifest.TotalSize = manifest.Files.Sum(file => file.Size);
        return manifest;
    }

    private static void MergeLegacyFailures(BackupManifest manifest, byte[] manifestBytes)
    {
        using var document = JsonDocument.Parse(manifestBytes);
        if (!document.RootElement.TryGetProperty("failedFiles", out var failedFiles) || failedFiles.ValueKind != JsonValueKind.Array) return;
        var known = manifest.Failures.Select(failure => NormalizeArchivePath(failure.RelativePath)).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var item in failedFiles.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object) continue;
            var path = GetString(item, "relativePath");
            if (string.IsNullOrWhiteSpace(path)) path = GetString(item, "path");
            if (string.IsNullOrWhiteSpace(path) || !known.Add(NormalizeArchivePath(path))) continue;
            manifest.Failures.Add(new BackupFailure
            {
                OriginalPath = GetString(item, "originalPath"),
                RelativePath = NormalizeArchivePath(path),
                Category = GetString(item, "category"),
                Module = GetString(item, "module"),
                ExceptionType = GetString(item, "exceptionType"),
                Error = GetString(item, "error")
            });
        }
    }

    private static string GetString(JsonElement element, string propertyName) =>
        element.TryGetProperty(propertyName, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() ?? "" : "";

    private static void NormalizeManifest(BackupManifest manifest)
    {
        manifest.Files ??= [];
        manifest.Failures ??= [];
        manifest.CriticalFiles ??= [];
        manifest.OptionalFiles ??= [];
        manifest.ExcludedRuntimeFiles ??= [];
        manifest.MissingCriticalFiles ??= [];
        manifest.ValidationIssues ??= [];
        manifest.Modules ??= [];
        manifest.ValidationIssues.Clear();
        manifest.Modules.Clear();
        manifest.MissingCriticalFiles.Clear();
    }

    private static void FinalizeManifest(BackupManifest manifest, HashSet<string> validPaths)
    {
        foreach (var failure in manifest.Failures) ApplyClassification(failure);
        foreach (var file in manifest.Files) ApplyClassification(file);
        foreach (var failure in manifest.Failures.Where(failure => failure.Module == BackupModules.RuntimeExcluded))
            if (!manifest.ExcludedRuntimeFiles.Contains(failure.RelativePath, StringComparer.OrdinalIgnoreCase))
                manifest.ExcludedRuntimeFiles.Add(failure.RelativePath);

        var moduleIds = manifest.Files.Select(file => file.Module)
            .Concat(manifest.Failures.Select(failure => failure.Module))
            .Concat(manifest.ValidationIssues.Select(issue => issue.Module))
            .Where(module => !string.IsNullOrWhiteSpace(module) && !module.Equals(BackupModules.RuntimeExcluded, StringComparison.OrdinalIgnoreCase))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        manifest.Modules = moduleIds.Select(module =>
        {
            var valid = manifest.Files.Count(file => file.Module.Equals(module, StringComparison.OrdinalIgnoreCase) &&
                                                     validPaths.Contains(NormalizeArchivePath(file.RelativePath)) &&
                                                     !IsExcludedRuntimePath(file.RelativePath));
            var missing = manifest.Failures.Where(failure => failure.Module.Equals(module, StringComparison.OrdinalIgnoreCase))
                .Select(failure => failure.RelativePath).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            var invalid = manifest.ValidationIssues.Where(issue => issue.Module.Equals(module, StringComparison.OrdinalIgnoreCase))
                .Select(issue => issue.RelativePath).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            return new BackupModuleInfo
            {
                Module = module,
                DisplayName = GetModuleDisplayName(module),
                ValidFileCount = valid,
                MissingFiles = missing,
                InvalidFiles = invalid
            };
        }).OrderBy(module => GetModuleSortOrder(module.Module)).ToList();

        var expectedCritical = manifest.CriticalFiles.Select(NormalizeArchivePath).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var failure in manifest.Failures.Where(failure => failure.IsCritical)) expectedCritical.Add(NormalizeArchivePath(failure.RelativePath));
        manifest.MissingCriticalFiles = expectedCritical.Where(path => !validPaths.Contains(path)).OrderBy(path => path).ToList();
        manifest.CanRestore = manifest.Modules.Any(module => module.CanRestore);
        manifest.Status = !manifest.CanRestore ? BackupStatuses.Incomplete :
            manifest.Failures.Count > 0 || manifest.ValidationIssues.Count > 0 ? BackupStatuses.CompleteWithWarnings : BackupStatuses.Complete;
    }

    private static void ApplyClassification(BackupFileRecord record)
    {
        var classification = Classify(record.RelativePath);
        if (string.IsNullOrWhiteSpace(record.Category)) record.Category = classification.Category;
        if (string.IsNullOrWhiteSpace(record.Module)) record.Module = classification.Module;
        if (classification.IsCritical) record.IsCritical = true;
    }

    private static void ApplyClassification(BackupFailure failure)
    {
        var classification = Classify(failure.RelativePath);
        if (string.IsNullOrWhiteSpace(failure.Category)) failure.Category = classification.Category;
        if (string.IsNullOrWhiteSpace(failure.Module)) failure.Module = classification.Module;
        if (classification.IsCritical) failure.IsCritical = true;
        if (string.IsNullOrWhiteSpace(failure.ExceptionType)) failure.ExceptionType = "UnknownException";
    }

    private static FileClassification Classify(string archivePath)
    {
        var path = NormalizeArchivePath(archivePath);
        if (IsExcludedRuntimePath(path)) return new FileClassification("运行时文件", BackupModules.RuntimeExcluded, false, true);

        if (path.Equals("bridge/roaming/settings.json", StringComparison.OrdinalIgnoreCase))
            return new FileClassification("用户设置", BackupModules.ApplicationSettings, true, false);
        if (path.Equals("bridge/local/bindings.json", StringComparison.OrdinalIgnoreCase))
            return new FileClassification("会话绑定", BackupModules.Bindings, true, false);
        if (path.Equals("bridge/local/data/commands.json", StringComparison.OrdinalIgnoreCase))
            return new FileClassification("指令配置", BackupModules.Commands, true, false);
        if (path.Equals("bridge/local/data/mirror-state.json", StringComparison.OrdinalIgnoreCase))
            return new FileClassification("消息同步配置", BackupModules.MessageSync, true, false);
        if (path.Equals("bridge/local/data/thread-numbers.json", StringComparison.OrdinalIgnoreCase))
            return new FileClassification("会话编号状态", BackupModules.ThreadState, true, false);
        if (path.Contains("/secrets/qq", StringComparison.OrdinalIgnoreCase))
            return new FileClassification("QQ 凭据", BackupModules.Qq, true, false);
        if (path.Contains("/secrets/telegram", StringComparison.OrdinalIgnoreCase))
            return new FileClassification("Telegram 凭据", BackupModules.Telegram, true, false);
        if (path.StartsWith("bridge/", StringComparison.OrdinalIgnoreCase))
            return new FileClassification("Bridge 持久化数据", BackupModules.OtherPersistentData, false, false);

        if (path.Equals("codex/config.toml", StringComparison.OrdinalIgnoreCase) ||
            path.Equals("codex/auth.json", StringComparison.OrdinalIgnoreCase) ||
            path.Equals("codex/.codex-global-state.json", StringComparison.OrdinalIgnoreCase) ||
            path.Equals("codex/AGENTS.md", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith("codex/rules/", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith("codex/agents/", StringComparison.OrdinalIgnoreCase))
            return new FileClassification("Codex 配置", BackupModules.CodexSettings, true, false);
        if (path.StartsWith("codex/sessions/", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith("codex/archived_sessions/", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith("codex/attachments/", StringComparison.OrdinalIgnoreCase) ||
            path.Equals("codex/session_index.jsonl", StringComparison.OrdinalIgnoreCase))
            return new FileClassification("Codex 会话与历史", BackupModules.Sessions, true, false);
        if (path.StartsWith("codex/skills/", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith("codex/plugins/", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith("codex/visualizations/", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith("codex/generated_images/", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith("codex/goals_", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith("codex/memories_", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith("codex/state_", StringComparison.OrdinalIgnoreCase))
            return new FileClassification("Codex 用户数据", BackupModules.OtherPersistentData, true, false);
        return new FileClassification("其他持久化数据", BackupModules.OtherPersistentData, false, false);
    }

    private static bool IsExcludedRuntimeDirectory(string archivePath)
    {
        var path = NormalizeArchivePath(archivePath);
        var parts = path.Split('/');
        if (parts.Any(part => RuntimeDirectoryNames.Contains(part))) return true;
        if (path.StartsWith("codex/plugins/cache", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith("codex/packages/", StringComparison.OrdinalIgnoreCase)) return true;
        return false;
    }

    private static bool IsExcludedRuntimePath(string archivePath)
    {
        var path = NormalizeArchivePath(archivePath);
        if (IsExcludedRuntimeDirectory(path)) return true;
        var fileName = Path.GetFileName(path);
        if (RuntimeExtensions.Contains(Path.GetExtension(fileName))) return true;
        if (fileName.EndsWith("-wal", StringComparison.OrdinalIgnoreCase) ||
            fileName.EndsWith("-shm", StringComparison.OrdinalIgnoreCase) ||
            fileName.EndsWith("-journal", StringComparison.OrdinalIgnoreCase)) return true;
        if (fileName.StartsWith("..codex-global-state.json.tmp-", StringComparison.OrdinalIgnoreCase) ||
            fileName.Equals("models_cache.json", StringComparison.OrdinalIgnoreCase) ||
            fileName.StartsWith("logs_", StringComparison.OrdinalIgnoreCase) && fileName.EndsWith(".sqlite", StringComparison.OrdinalIgnoreCase)) return true;
        return false;
    }

    private static BackupFailure CreateFailure(string originalPath, string archivePath, FileClassification classification, Exception exception) => new()
    {
        OriginalPath = originalPath,
        RelativePath = NormalizeArchivePath(archivePath),
        Category = classification.Category,
        Module = classification.Module,
        ExceptionType = exception.GetType().Name,
        Error = exception.Message,
        IsCritical = classification.IsCritical
    };

    private void LogFailure(BackupFailure failure)
    {
        _logs?.Add("backup", $"failed:{Environment.NewLine}" +
            $"original={failure.OriginalPath}{Environment.NewLine}" +
            $"relative={failure.RelativePath}{Environment.NewLine}" +
            $"category={failure.Category}{Environment.NewLine}" +
            $"module={GetModuleDisplayName(failure.Module)}{Environment.NewLine}" +
            $"{failure.ExceptionType}: {failure.Error}{Environment.NewLine}" +
            $"critical={failure.IsCritical.ToString().ToLowerInvariant()}");
    }

    private static void AddValidationIssue(BackupManifest manifest, string path, string module, string error)
    {
        manifest.ValidationIssues.Add(new BackupValidationIssue
        {
            RelativePath = NormalizeArchivePath(path),
            Module = module,
            Error = error
        });
    }

    private static bool MatchesRecordedFailure(string path, IEnumerable<BackupFailure> failures) => failures.Any(failure =>
    {
        var failed = NormalizeArchivePath(failure.RelativePath);
        return failed.EndsWith("/**", StringComparison.Ordinal) ? path.StartsWith(failed[..^2], StringComparison.OrdinalIgnoreCase) :
            path.Equals(failed, StringComparison.OrdinalIgnoreCase);
    });

    private static bool IsSelected(string path, RestoreOptions options) =>
        options.RestoreCodex && NormalizeArchivePath(path).StartsWith("codex/", StringComparison.OrdinalIgnoreCase) ||
        options.RestoreBridge && NormalizeArchivePath(path).StartsWith("bridge/", StringComparison.OrdinalIgnoreCase);

    private static bool IsModuleSelected(string module, RestoreOptions options) => module == BackupModules.CodexSettings ||
        module == BackupModules.Sessions ? options.RestoreCodex :
        module == BackupModules.ApplicationSettings || module == BackupModules.Qq || module == BackupModules.Telegram ||
        module == BackupModules.Bindings || module == BackupModules.Commands || module == BackupModules.MessageSync ||
        module == BackupModules.ThreadState ? options.RestoreBridge : options.RestoreCodex || options.RestoreBridge;

    private static int GetModuleSortOrder(string module) => module switch
    {
        BackupModules.ApplicationSettings => 0,
        BackupModules.CodexSettings => 1,
        BackupModules.Qq => 2,
        BackupModules.Telegram => 3,
        BackupModules.Bindings => 4,
        BackupModules.Commands => 5,
        BackupModules.MessageSync => 6,
        BackupModules.ThreadState => 7,
        BackupModules.Sessions => 8,
        _ => 9
    };

    private static async Task<FileStream> OpenReadWithRetryAsync(string path, CancellationToken cancellationToken)
    {
        Exception? last = null;
        for (var attempt = 0; attempt < 3; attempt++)
        {
            try { return new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 131072, true); }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                last = exception;
                if (attempt < 2) await Task.Delay(200 * (attempt + 1), cancellationToken);
            }
        }
        throw last!;
    }

    private static string TryGetCodexVersion()
    {
        try
        {
            using var process = Process.Start(new ProcessStartInfo("codex", "--version") { RedirectStandardOutput = true, UseShellExecute = false, CreateNoWindow = true });
            if (process is null) return "";
            if (!process.WaitForExit(2000)) { process.Kill(); return ""; }
            return process.StandardOutput.ReadToEnd().Trim();
        }
        catch { return ""; }
    }

    private static bool IsValidSha256(string value)
    {
        if (value.Length != 64) return false;
        try { _ = Convert.FromHexString(value); return true; }
        catch (FormatException) { return false; }
    }

    private static string NormalizeArchivePath(string path) => path.Replace('\\', '/').TrimStart('/');
    private static bool IsAllowedArchivePath(string path) =>
        (path.StartsWith("codex/", StringComparison.OrdinalIgnoreCase) ||
         path.StartsWith("bridge/local/", StringComparison.OrdinalIgnoreCase) ||
         path.StartsWith("bridge/roaming/", StringComparison.OrdinalIgnoreCase)) &&
        !path.Split('/').Any(part => part is "" or "." or "..");
    private static bool IsWithin(string path, string root)
    {
        var fullPath = Path.GetFullPath(path);
        var fullRoot = Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        return fullPath.StartsWith(fullRoot, StringComparison.OrdinalIgnoreCase);
    }
    private static DateTimeOffset ClampZipTime(DateTimeOffset value) => value.Year < 1980 ? new DateTimeOffset(1980, 1, 1, 0, 0, 0, TimeSpan.Zero) : value;
    private static void TryDeleteFile(string path) { try { if (File.Exists(path)) File.Delete(path); } catch { } }
    private static void TryDeleteDirectory(string path) { try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); } catch { } }

    private sealed record BackupSource(string Root, string Prefix);
    private sealed record FileClassification(string Category, string Module, bool IsCritical, bool Excluded);
    private sealed record SourceFile(string FullPath, string ArchivePath, long Size, DateTimeOffset LastWriteTime, FileClassification Classification);
    private sealed class ScanResult
    {
        public List<SourceFile> Files { get; } = [];
        public List<BackupFailure> Failures { get; } = [];
        public List<string> ExcludedRuntimeFiles { get; } = [];
    }
    private sealed class FileCapture(FileStream stream, string? temporaryPath) : IAsyncDisposable
    {
        public FileStream Stream { get; } = stream;
        public async ValueTask DisposeAsync()
        {
            await Stream.DisposeAsync();
            if (temporaryPath is not null) TryDeleteFile(temporaryPath);
        }
    }
}
