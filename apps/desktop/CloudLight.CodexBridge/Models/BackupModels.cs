namespace CloudLight.CodexBridge.Models;

public sealed class BackupManifest
{
    public int FormatVersion { get; set; } = 1;
    public DateTimeOffset CreatedAt { get; set; }
    public string AppVersion { get; set; } = "";
    public string CodexVersion { get; set; } = "";
    public string MachineName { get; set; } = "";
    public string CodexHome { get; set; } = "";
    public bool IncludedCodex { get; set; }
    public bool IncludedBridge { get; set; }
    public int FileCount { get; set; }
    public long TotalSize { get; set; }
    public string Status { get; set; } = BackupStatuses.Complete;
    public bool CanRestore { get; set; } = true;
    public List<BackupFileRecord> Files { get; set; } = [];
    public List<BackupFailure> Failures { get; set; } = [];
    public List<string> CriticalFiles { get; set; } = [];
    public List<string> OptionalFiles { get; set; } = [];
    public List<string> ExcludedRuntimeFiles { get; set; } = [];
    public List<string> MissingCriticalFiles { get; set; } = [];
    public List<BackupValidationIssue> ValidationIssues { get; set; } = [];
    public List<BackupModuleInfo> Modules { get; set; } = [];
}

public sealed class BackupFileRecord
{
    public string RelativePath { get; set; } = "";
    public long Size { get; set; }
    public string Sha256 { get; set; } = "";
    public DateTimeOffset LastWriteTime { get; set; }
    public string Category { get; set; } = "";
    public string Module { get; set; } = "";
    public bool IsCritical { get; set; }
}

public sealed class BackupFailure
{
    public string OriginalPath { get; set; } = "";
    public string RelativePath { get; set; } = "";
    public string Category { get; set; } = "";
    public string Module { get; set; } = "";
    public string ExceptionType { get; set; } = "";
    public string Error { get; set; } = "";
    public bool IsCritical { get; set; }
}

public sealed class BackupValidationIssue
{
    public string RelativePath { get; set; } = "";
    public string Module { get; set; } = "";
    public string Error { get; set; } = "";
}

public sealed class BackupModuleInfo
{
    public string Module { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public int ValidFileCount { get; set; }
    public List<string> MissingFiles { get; set; } = [];
    public List<string> InvalidFiles { get; set; } = [];
    public bool CanRestore => ValidFileCount > 0;
    public string Status => CanRestore
        ? MissingFiles.Count + InvalidFiles.Count > 0 ? "Partial" : "Complete"
        : "Unavailable";
}

public static class BackupModules
{
    public const string ApplicationSettings = "application-settings";
    public const string CodexSettings = "codex-settings";
    public const string Qq = "qq";
    public const string Telegram = "telegram";
    public const string Bindings = "bindings";
    public const string Commands = "commands";
    public const string MessageSync = "message-sync";
    public const string ThreadState = "thread-state";
    public const string TaskCenter = "task-center";
    public const string Sessions = "sessions";
    public const string OtherPersistentData = "other-persistent-data";
    public const string RuntimeExcluded = "runtime-excluded";
}

public static class BackupStatuses
{
    public const string Complete = "Complete";
    public const string CompleteWithWarnings = "CompleteWithWarnings";
    public const string Incomplete = "Incomplete";
}

public sealed record BackupProgress(string Stage, int ProcessedFiles, int TotalFiles, long ProcessedBytes, long TotalBytes)
{
    public int Percent => TotalBytes > 0 ? (int)Math.Clamp(ProcessedBytes * 100L / TotalBytes, 0, 100) : 0;
}

public sealed class BackupResult
{
    public required string FilePath { get; init; }
    public required BackupManifest Manifest { get; init; }
    public bool IsComplete => Manifest.Status == BackupStatuses.Complete;
    public bool HasWarnings => Manifest.Status == BackupStatuses.CompleteWithWarnings;
    public bool IsRestorable => Manifest.CanRestore;
}

public sealed class BackupCreationException(string message, BackupManifest manifest) : IOException(message)
{
    public BackupManifest Manifest { get; } = manifest;
}

public sealed class RestoreOptions
{
    public bool RestoreCodex { get; set; } = true;
    public bool RestoreBridge { get; set; } = true;
    public bool Replace { get; set; } = true;
    public bool VerifyNoExternalCodex { get; set; } = true;
    public required string PreRestoreDirectory { get; set; }
}

public sealed class RestoreResult
{
    public required BackupManifest Manifest { get; init; }
    public required string PreRestoreBackupPath { get; init; }
    public int RestoredFiles { get; init; }
    public List<string> SucceededModules { get; init; } = [];
    public List<string> FailedModules { get; init; } = [];
    public List<BackupRestoreWarning> Warnings { get; init; } = [];
    public bool IsPartial => FailedModules.Count > 0 || Warnings.Count > 0;
}

public sealed class BackupRestoreWarning
{
    public string RelativePath { get; set; } = "";
    public string Module { get; set; } = "";
    public string Error { get; set; } = "";
    public bool AffectsOtherModules { get; set; }
}
