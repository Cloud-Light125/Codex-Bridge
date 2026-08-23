using System.IO.Compression;
using System.Security.Cryptography;
using System.Text.Json;
using CloudLight.CodexBridge.Services;
using CloudLight.CodexBridge.Models;
using Microsoft.Win32;

if (args.Contains("--codex-discovery-retry-tests", StringComparer.OrdinalIgnoreCase))
{
    var logs = new LogService();
    var retrySettings = new UserSettings
    {
        CodexCustomPath = @"C:\Users\test\manual-codex.exe",
        DetectedCodexPath = @"C:\Users\test\previous-detected-codex.exe"
    };
    var failed = new CodexDiscoveryResult(false, "", "", CodexDiscoverySource.None);
    Assert(!CodexPathSettings.RememberAutomaticDiscovery(retrySettings, failed),
        "失败的瞬时 discovery 不得修改路径设置");
    Assert(retrySettings.CodexCustomPath == @"C:\Users\test\manual-codex.exe",
        "失败的瞬时 discovery 不得清空用户手动路径");

    var attemptCount = 0;
    var applyCount = 0;
    var refreshCount = 0;
    var recoveredPath = @"C:\Program Files\OpenAI\codex.exe";
    var runner = new CodexDiscoveryRetryRunner(logs,
        [TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero]);
    var recovered = await runner.RunAsync(
        _ => Task.FromResult(++attemptCount < 3
            ? failed
            : new CodexDiscoveryResult(true, recoveredPath, "codex-cli test", CodexDiscoverySource.ChatGPTProcess)),
        (discovery, _) =>
        {
            applyCount++;
            Assert(discovery.Path == recoveredPath, "重试成功后必须应用本次发现的路径");
            refreshCount++;
            return Task.CompletedTask;
        },
        CancellationToken.None);
    Assert(recovered && attemptCount == 3, "前两次失败、第三次成功时必须在当前进程恢复");
    Assert(applyCount == 1 && refreshCount == 1, "恢复后必须且只能调用一次 ApplyCodexPath 和 UI 刷新");

    var periodicAttempts = 0;
    var periodicRunner = new CodexDiscoveryRetryRunner(logs, [TimeSpan.Zero, TimeSpan.Zero]);
    Assert(await periodicRunner.RunAsync(
            _ => Task.FromResult(++periodicAttempts < 5
                ? failed
                : new CodexDiscoveryResult(true, recoveredPath, "codex-cli test", CodexDiscoverySource.PATH)),
            (_, _) => Task.CompletedTask,
            CancellationToken.None) && periodicAttempts == 5,
        "退避级别用尽后必须按最大间隔继续检测，而不是停止 retry");

    var automatic = new CodexDiscoveryResult(true, recoveredPath, "codex-cli test", CodexDiscoverySource.ChatGPTProcess);
    Assert(CodexPathSettings.RememberAutomaticDiscovery(retrySettings, automatic), "自动发现路径必须单独保存");
    Assert(retrySettings.CodexCustomPath == @"C:\Users\test\manual-codex.exe" && retrySettings.DetectedCodexPath == recoveredPath,
        "自动发现结果不得覆盖用户手动路径");

    using var cancellation = new CancellationTokenSource();
    var cancellationRunner = new CodexDiscoveryRetryRunner(logs, [TimeSpan.FromMinutes(1)]);
    var cancellationTask = cancellationRunner.RunAsync(_ => Task.FromResult(failed), (_, _) => Task.CompletedTask, cancellation.Token);
    cancellation.Cancel();
    var cancelled = false;
    try { await cancellationTask; }
    catch (OperationCanceledException) { cancelled = true; }
    Assert(cancelled, "应用退出取消令牌必须立即终止 retry");

    Console.WriteLine("PASS Codex discovery retry recovery, path preservation, apply/refresh, cancellation");
    return;
}

if (args.Length == 2 && args[0].Equals("--validate-backup", StringComparison.OrdinalIgnoreCase))
{
    var manifest = await new BackupService(new SettingsService()).ReadAndValidateAsync(args[1]);
    Console.WriteLine($"PASS backup validation: status={manifest.Status} canRestore={manifest.CanRestore} failures={manifest.Failures.Count} issues={manifest.ValidationIssues.Count}");
    foreach (var failure in manifest.Failures)
        Console.WriteLine($"FAILED {failure.RelativePath} | {failure.ExceptionType}: {failure.Error} | critical={failure.IsCritical} | module={BackupService.GetModuleDisplayName(failure.Module)}");
    foreach (var module in manifest.Modules)
        Console.WriteLine($"MODULE {module.DisplayName} | status={module.Status} | validFiles={module.ValidFileCount}");
    return;
}

if (args.Contains("--live-codex-discovery", StringComparer.OrdinalIgnoreCase))
{
    var originalPath = Environment.GetEnvironmentVariable("PATH");
    try
    {
        Environment.SetEnvironmentVariable("PATH", "");
        var liveDiscovery = await new CodexDiscoveryService(new LogService())
            .DiscoverAsync(@"Z:\missing-codex.exe");
        Assert(liveDiscovery.Found && liveDiscovery.Source is CodexDiscoverySource.CodexProcess or CodexDiscoverySource.ChatGPTProcess,
            "清空 PATH 后必须能从当前 Codex/ChatGPT 进程发现有效 Codex");
        Console.WriteLine($"PASS live Codex discovery: {liveDiscovery.Source} {liveDiscovery.Path} {liveDiscovery.Version}");
        return;
    }
    finally
    {
        Environment.SetEnvironmentVariable("PATH", originalPath);
    }
}

var root = Path.Combine(Path.GetTempPath(), $"CloudLight-CodexBridge-Smoke-{Guid.NewGuid():N}");
var codex = Path.Combine(root, "codex-current");
var bridgeLocal = Path.Combine(root, "bridge-local");
var bridgeRoaming = Path.Combine(root, "bridge-roaming");
var backups = Path.Combine(root, "backups");
Directory.CreateDirectory(Path.Combine(codex, "sessions"));
Directory.CreateDirectory(Path.Combine(codex, "nested"));
Directory.CreateDirectory(Path.Combine(codex, "cache"));
Directory.CreateDirectory(Path.Combine(codex, "tmp", "arg0", "active"));
Directory.CreateDirectory(Path.Combine(bridgeLocal, "data"));
Directory.CreateDirectory(Path.Combine(bridgeLocal, "logs"));
Directory.CreateDirectory(Path.Combine(bridgeLocal, "secrets"));
Directory.CreateDirectory(bridgeRoaming);
await File.WriteAllTextAsync(Path.Combine(codex, "sessions", "a.jsonl"), "{\"id\":1}\n");
await File.WriteAllTextAsync(Path.Combine(codex, "config.toml"), "model = \"gpt-test\"\n");
await File.WriteAllTextAsync(Path.Combine(codex, "state.json"), "{\"ok\":true}");
await File.WriteAllBytesAsync(Path.Combine(codex, "nested", "test.db"), Enumerable.Range(0, 4096).Select(value => (byte)(value % 251)).ToArray());
await File.WriteAllTextAsync(Path.Combine(codex, "cache", "models.json"), "runtime");
await File.WriteAllTextAsync(Path.Combine(codex, "tmp", "arg0", "active", ".lock"), "runtime");
await File.WriteAllTextAsync(Path.Combine(bridgeLocal, "bindings.json"), "[{\"id\":\"binding-a\"}]");
await File.WriteAllTextAsync(Path.Combine(bridgeLocal, "data", "commands.json"), "{\"schemaVersion\":1,\"commands\":[]}");
await File.WriteAllTextAsync(Path.Combine(bridgeLocal, "data", "mirror-state.json"), "{\"cursor\":9}");
await File.WriteAllTextAsync(Path.Combine(bridgeLocal, "data", "thread-numbers.json"), "{\"thread-a\":41}");
await File.WriteAllBytesAsync(Path.Combine(bridgeLocal, "secrets", "qqbot-app-secret.dat"), [1, 2, 3, 4]);
await File.WriteAllBytesAsync(Path.Combine(bridgeLocal, "secrets", "telegram-token.dat"), [5, 6, 7, 8]);
await File.WriteAllTextAsync(Path.Combine(bridgeLocal, "logs", "bridge-daemon.log"), "runtime");
await File.WriteAllTextAsync(Path.Combine(bridgeRoaming, "settings.json"), "{\"closeToTray\":true}");

var settings = new SettingsService();
var service = new BackupService(settings, codex, bridgeLocal, bridgeRoaming);
var backupPath = Path.Combine(backups, "roundtrip.clcbak");
var result = await service.CreateBackupAsync(backupPath, true, true);
Assert(result.IsComplete, "备份必须完整成功");
Assert(result.Manifest.FileCount == 11, $"预期 11 个持久化文件，实际 {result.Manifest.FileCount}");
Assert(result.Manifest.ExcludedRuntimeFiles.Any(path => path.Contains("cache", StringComparison.OrdinalIgnoreCase)), "cache 必须在创建阶段排除");
Assert(result.Manifest.ExcludedRuntimeFiles.Any(path => path.Contains("tmp", StringComparison.OrdinalIgnoreCase)), "tmp/lock 必须在创建阶段排除");
Assert(result.Manifest.ExcludedRuntimeFiles.Any(path => path.Contains("logs", StringComparison.OrdinalIgnoreCase)), "日志必须在创建阶段排除");
var expected = Snapshot(codex, bridgeLocal, bridgeRoaming);

await File.WriteAllTextAsync(Path.Combine(codex, "config.toml"), "changed");
File.Delete(Path.Combine(codex, "sessions", "a.jsonl"));
await File.WriteAllTextAsync(Path.Combine(codex, "extra.txt"), "must disappear");
await File.WriteAllTextAsync(Path.Combine(bridgeLocal, "bindings.json"), "[]");
var stopCount = 0;
var restartCount = 0;
var restore = await service.RestoreAsync(backupPath, new RestoreOptions
{
    RestoreCodex = true, RestoreBridge = true, Replace = true,
    VerifyNoExternalCodex = false, PreRestoreDirectory = backups
}, () => { stopCount++; return Task.CompletedTask; }, () => { restartCount++; return Task.CompletedTask; });
Assert(File.Exists(restore.PreRestoreBackupPath), "完整替换前必须生成 PreRestore 备份");
Assert(stopCount == 1 && restartCount == 1, "一次恢复只能暂停并重新初始化运行服务各一次");
Assert(File.Exists(Path.Combine(codex, "extra.txt")), "容错恢复不得删除未纳入恢复计划的现有数据");
var actual = Snapshot(codex, bridgeLocal, bridgeRoaming);
foreach (var item in expected)
{
    if (item.Key.Contains("cache/", StringComparison.OrdinalIgnoreCase) || item.Key.Contains("tmp/", StringComparison.OrdinalIgnoreCase) || item.Key.Contains("logs/", StringComparison.OrdinalIgnoreCase)) continue;
    Assert(actual.TryGetValue(item.Key, out var hash) && hash == item.Value, $"恢复后持久化数据 SHA-256 不一致：{item.Key}");
}

var corrupt = Path.Combine(backups, "corrupt.clcbak");
File.Copy(backupPath, corrupt);
using (var archive = ZipFile.Open(corrupt, ZipArchiveMode.Update))
{
    var entry = archive.GetEntry("codex/config.toml")!;
    entry.Delete();
    var changed = archive.CreateEntry("codex/config.toml");
    await using var writer = new StreamWriter(changed.Open());
    await writer.WriteAsync("corrupted");
}
var tolerant = await service.ReadAndValidateAsync(corrupt);
Assert(tolerant.CanRestore, "单个 Codex 配置损坏时其他模块仍必须可恢复");
Assert(tolerant.ValidationIssues.Any(issue => issue.RelativePath == "codex/config.toml"), "损坏文件必须出现在验证警告中");

var partialCreation = Path.Combine(backups, "partial-creation.clcbak");
await using (var lockedConfig = new FileStream(Path.Combine(codex, "config.toml"), FileMode.Open, FileAccess.ReadWrite, FileShare.None))
{
    var partial = await service.CreateBackupAsync(partialCreation, true, true);
    Assert(partial.HasWarnings && partial.IsRestorable, "单个模块文件读取失败时必须保留仍可恢复的部分备份");
    Assert(partial.Manifest.Failures.Any(failure => failure.RelativePath == "codex/config.toml" && failure.IsCritical), "创建失败详情必须包含关键文件分类");
}

var damagedSettings = Path.Combine(backups, "damaged-settings.clcbak");
File.Copy(backupPath, damagedSettings);
using (var archive = ZipFile.Open(damagedSettings, ZipArchiveMode.Update))
{
    archive.GetEntry("bridge/roaming/settings.json")!.Delete();
    var changed = archive.CreateEntry("bridge/roaming/settings.json");
    await using var writer = new StreamWriter(changed.Open());
    await writer.WriteAsync("{broken-json");
}
await File.WriteAllTextAsync(Path.Combine(bridgeRoaming, "settings.json"), "{\"keepCurrent\":true}");
await File.WriteAllTextAsync(Path.Combine(bridgeLocal, "bindings.json"), "[]");
await File.WriteAllTextAsync(Path.Combine(bridgeLocal, "data", "commands.json"), "{\"schemaVersion\":1,\"commands\":[{\"changed\":true}]}");
var partialRestore = await service.RestoreAsync(damagedSettings, new RestoreOptions
{
    RestoreCodex = false, RestoreBridge = true, Replace = true,
    VerifyNoExternalCodex = false, PreRestoreDirectory = backups
}, null, null);
Assert(partialRestore.IsPartial, "单个 JSON 损坏必须返回部分恢复报告");
Assert(partialRestore.SucceededModules.Contains(BackupModules.Bindings) && partialRestore.SucceededModules.Contains(BackupModules.Commands), "损坏应用设置不得阻止 bindings/commands 恢复");
Assert((await File.ReadAllTextAsync(Path.Combine(bridgeRoaming, "settings.json"))).Contains("keepCurrent"), "损坏的 settings.json 不得覆盖当前设置");
Assert((await File.ReadAllTextAsync(Path.Combine(bridgeLocal, "bindings.json"))).Contains("binding-a"), "有效 bindings 必须继续恢复");

var legacy = Path.Combine(backups, "legacy-failed-files.clcbak");
File.Copy(backupPath, legacy);
using (var archive = ZipFile.Open(legacy, ZipArchiveMode.Update))
{
    var manifestEntry = archive.GetEntry("manifest.json")!;
    BackupManifest legacyManifest;
    await using (var stream = manifestEntry.Open()) legacyManifest = (await JsonSerializer.DeserializeAsync<BackupManifest>(stream, new JsonSerializerOptions { PropertyNameCaseInsensitive = true }))!;
    manifestEntry.Delete();
    legacyManifest.FormatVersion = 1;
    legacyManifest.Failures.Add(new BackupFailure { RelativePath = "codex/thread-writer-locks/legacy.lock", Error = "另一个程序已锁定文件的一部分" });
    var failedEntry = archive.CreateEntry("codex/thread-writer-locks/legacy.lock");
    await using (failedEntry.Open()) { }
    var replacement = archive.CreateEntry("manifest.json");
    await using var target = replacement.Open();
    await JsonSerializer.SerializeAsync(target, legacyManifest, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
}
var legacyResult = await service.ReadAndValidateAsync(legacy);
Assert(legacyResult.CanRestore && legacyResult.Status == BackupStatuses.CompleteWithWarnings, "旧 manifest 的 failedFiles 必须作为警告并继续识别数据");

var brokenZip = Path.Combine(backups, "broken-zip.clcbak");
await File.WriteAllBytesAsync(brokenZip, [1, 2, 3, 4, 5]);
var brokenRejected = false;
try { await service.ReadAndValidateAsync(brokenZip); } catch (InvalidDataException) { brokenRejected = true; }
Assert(brokenRejected, "ZIP 容器损坏必须拒绝恢复");

var runtimeOnly = Path.Combine(backups, "runtime-only.clcbak");
using (var archive = ZipFile.Open(runtimeOnly, ZipArchiveMode.Create))
{
    var entry = archive.CreateEntry("codex/logs/only.log");
    await using var writer = new StreamWriter(entry.Open());
    await writer.WriteAsync("runtime");
}
var emptyRejected = false;
try { await service.ReadAndValidateAsync(runtimeOnly); } catch (InvalidDataException) { emptyRejected = true; }
Assert(emptyRejected, "只有运行时文件、没有任何可恢复数据时必须拒绝恢复");

const string runKey = @"Software\Microsoft\Windows\CurrentVersion\Run";
object? previous;
using (var key = Registry.CurrentUser.OpenSubKey(runKey)) previous = key?.GetValue(StartupService.ValueName);
try
{
    var startup = new StartupService();
    startup.Configure(true, true);
    using (var key = Registry.CurrentUser.OpenSubKey(runKey))
    {
        var value = key?.GetValue(StartupService.ValueName)?.ToString() ?? "";
        Assert(value.Contains("--silent", StringComparison.Ordinal), "启动项必须包含 --silent");
    }
    startup.Configure(false, true);
    Assert(!startup.IsEnabled, "关闭开机自启后启动项必须删除");
}
finally
{
    using var key = Registry.CurrentUser.CreateSubKey(runKey, true)!;
    if (previous is null) key.DeleteValue(StartupService.ValueName, false);
    else key.SetValue(StartupService.ValueName, previous);
}

Directory.Delete(root, true);
Console.WriteLine("PASS backup/restore SHA-256 roundtrip, corrupt rejection, PreRestore, startup registry");
return;

static Dictionary<string, string> Snapshot(params string[] roots)
{
    var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    for (var index = 0; index < roots.Length; index++)
        foreach (var file in Directory.EnumerateFiles(roots[index], "*", SearchOption.AllDirectories))
            result[$"{index}/{Path.GetRelativePath(roots[index], file).Replace('\\', '/')}"] = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(file)));
    return result;
}

static void Assert(bool condition, string message)
{
    if (!condition) throw new InvalidOperationException(message);
}
