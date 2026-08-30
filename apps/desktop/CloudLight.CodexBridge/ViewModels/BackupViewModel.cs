using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Reflection;
using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;
using Microsoft.Win32;

namespace CloudLight.CodexBridge.ViewModels;

public sealed class BackupViewModel : ObservableObject
{
    private readonly BackupService _service;
    private readonly LogService _logs;
    private CancellationTokenSource? _backupCancellation;
    private bool _includeCodex = true;
    private bool _includeBridge = true;
    private bool _restoreCodex = true;
    private bool _restoreBridge = true;
    private bool _replaceMode = true;
    private bool _busy;
    private string _selectedBackup = "";
    private BackupManifest? _selectedManifest;
    private string _operationText = "尚未执行备份或恢复";
    private string _progressStage = "准备就绪";
    private int _progressPercent;
    private string _dataSummary = "正在扫描…";

    public BackupViewModel(BackupService service, LogService logs)
    {
        _service = service;
        _logs = logs;
        CreateBackupCommand = new AsyncRelayCommand(CreateBackupAsync, () => !Busy);
        CancelBackupCommand = new RelayCommand(_ => _backupCancellation?.Cancel(), _ => Busy && _backupCancellation is not null);
        SelectBackupCommand = new AsyncRelayCommand(SelectBackupAsync, () => !Busy);
        RestoreCommand = new AsyncRelayCommand(RestoreAsync, () => !Busy && SelectedManifest is not null);
        OpenCodexHomeCommand = new RelayCommand(_ => OpenDirectory(CodexHome));
        _ = RefreshDataSummaryAsync();
    }

    public Func<Task>? StopRuntimeAsync { get; set; }
    public Func<Task>? RestartRuntimeAsync { get; set; }
    public Func<Task>? RefreshThreadsAsync { get; set; }
    public ICommand CreateBackupCommand { get; }
    public ICommand CancelBackupCommand { get; }
    public ICommand SelectBackupCommand { get; }
    public ICommand RestoreCommand { get; }
    public ICommand OpenCodexHomeCommand { get; }
    public ObservableCollection<string> RecentOperations { get; } = [];
    public string CodexHome => _service.CodexHome;
    public string BridgeData => _service.BridgeLocalData;
    public string AppVersion => Assembly.GetExecutingAssembly().GetName().Version?.ToString(3) ?? "1.1.4";

    public bool IncludeCodex { get => _includeCodex; set => SetProperty(ref _includeCodex, value); }
    public bool IncludeBridge { get => _includeBridge; set => SetProperty(ref _includeBridge, value); }
    public bool RestoreCodex { get => _restoreCodex; set => SetProperty(ref _restoreCodex, value); }
    public bool RestoreBridge { get => _restoreBridge; set => SetProperty(ref _restoreBridge, value); }
    public bool ReplaceMode { get => _replaceMode; set => SetProperty(ref _replaceMode, value); }
    public bool Busy { get => _busy; private set { if (SetProperty(ref _busy, value)) OnPropertyChanged(nameof(BusyVisibility)); } }
    public Visibility BusyVisibility => Busy ? Visibility.Visible : Visibility.Collapsed;
    public string SelectedBackup { get => _selectedBackup; private set => SetProperty(ref _selectedBackup, value); }
    public BackupManifest? SelectedManifest
    {
        get => _selectedManifest;
        private set
        {
            if (!SetProperty(ref _selectedManifest, value)) return;
            OnPropertyChanged(nameof(BackupDetails));
            OnPropertyChanged(nameof(BackupDetailsVisibility));
        }
    }
    public string BackupDetails => SelectedManifest is null ? "" : BuildBackupDetails(SelectedManifest);
    public Visibility BackupDetailsVisibility => SelectedManifest is null ? Visibility.Collapsed : Visibility.Visible;
    public string OperationText { get => _operationText; private set => SetProperty(ref _operationText, value); }
    public string ProgressStage { get => _progressStage; private set => SetProperty(ref _progressStage, value); }
    public int ProgressPercent { get => _progressPercent; private set => SetProperty(ref _progressPercent, value); }
    public string DataSummary { get => _dataSummary; private set => SetProperty(ref _dataSummary, value); }

    private async Task RefreshDataSummaryAsync()
    {
        try
        {
            var scan = await _service.ScanAsync(true, true);
            DataSummary = $"{scan.Files:N0} 个文件 · {FormatSize(scan.Size)}";
        }
        catch (Exception exception) { DataSummary = UiText.UserError(exception, "扫描数据"); }
    }

    private async Task CreateBackupAsync()
    {
        if (!IncludeCodex && !IncludeBridge) { OperationText = "请至少选择一项备份内容。"; return; }
        var dialog = new Microsoft.Win32.SaveFileDialog
        {
            Title = "创建完整备份",
            Filter = "CloudLight Codex Backup (*.clcbak)|*.clcbak",
            DefaultExt = ".clcbak",
            AddExtension = true,
            FileName = $"CloudLight-Codex-Backup-{DateTime.Now:yyyy-MM-dd-HHmmss}.clcbak"
        };
        if (dialog.ShowDialog() != true) return;
        if (System.Windows.MessageBox.Show("完整备份可能包含账号凭据，请妥善保存备份文件。", "完整备份提示", MessageBoxButton.OKCancel, MessageBoxImage.Warning) != MessageBoxResult.OK) return;
        Busy = true;
        _backupCancellation = new CancellationTokenSource();
        try
        {
            var result = await _service.CreateBackupAsync(dialog.FileName, IncludeCodex, IncludeBridge, CreateProgress(), _backupCancellation.Token);
            OperationText = result.HasWarnings
                ? $"备份完成（有警告）：成功保存 {result.Manifest.FileCount:N0} 个文件；{result.Manifest.Failures.Count} 个文件未保存，仍可恢复 {result.Manifest.Modules.Count(module => module.CanRestore)} 个数据模块。"
                : $"备份成功：{result.Manifest.FileCount:N0} 个文件，{FormatSize(result.Manifest.TotalSize)}。";
            AddRecent(OperationText);
        }
        catch (OperationCanceledException) { OperationText = "备份已取消，未保留未完成文件。"; }
        catch (Exception exception) { OperationText = UiText.UserError(exception, "创建备份"); _logs.AddException("backup", "创建完整备份失败。", exception); }
        finally { _backupCancellation.Dispose(); _backupCancellation = null; Busy = false; }
    }

    private async Task SelectBackupAsync()
    {
        var dialog = new Microsoft.Win32.OpenFileDialog { Title = "选择完整备份", Filter = "CloudLight Codex Backup (*.clcbak)|*.clcbak" };
        if (dialog.ShowDialog() != true) return;
        Busy = true;
        try
        {
            SelectedManifest = await _service.ReadAndValidateAsync(dialog.FileName, CreateProgress());
            SelectedBackup = dialog.FileName;
            RestoreCodex = SelectedManifest.IncludedCodex;
            RestoreBridge = SelectedManifest.IncludedBridge;
            OperationText = SelectedManifest.Status == BackupStatuses.Complete
                ? "备份验证完成，可以开始恢复。"
                : $"该备份包含可恢复数据，但有 {SelectedManifest.Failures.Count + SelectedManifest.ValidationIssues.Count} 项警告；恢复时会跳过无效项并继续恢复其他模块。";
        }
        catch (Exception exception)
        {
            SelectedManifest = null;
            SelectedBackup = "";
            OperationText = $"备份无法安全识别，未进入恢复流程：{UiText.UserError(exception, "读取备份")}";
            _logs.AddException("backup", "读取备份文件失败。", exception);
        }
        finally { Busy = false; }
    }

    private async Task RestoreAsync()
    {
        if (SelectedManifest is null || string.IsNullOrWhiteSpace(SelectedBackup)) return;
        if (!RestoreCodex && !RestoreBridge) { OperationText = "请至少选择一项恢复内容。"; return; }
        var mode = ReplaceMode ? "完整替换" : "合并";
        if (System.Windows.MessageBox.Show($"即将以“{mode}”方式恢复。恢复前会自动备份当前数据，以便需要时回退。是否继续？", "确认恢复", MessageBoxButton.YesNo, MessageBoxImage.Warning) != MessageBoxResult.Yes) return;
        Busy = true;
        try
        {
            var preRestoreDirectory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "CloudLight Codex Backups");
            var result = await _service.RestoreAsync(SelectedBackup, new RestoreOptions
            {
                RestoreCodex = RestoreCodex,
                RestoreBridge = RestoreBridge,
                Replace = ReplaceMode,
                PreRestoreDirectory = preRestoreDirectory
            }, StopRuntimeAsync, RestartRuntimeAsync, CreateProgress());
            if (RefreshThreadsAsync is not null) await RefreshThreadsAsync();
            var succeeded = string.Join("、", result.SucceededModules.Select(BackupService.GetModuleDisplayName));
            var failed = string.Join("、", result.FailedModules.Select(BackupService.GetModuleDisplayName));
            var warningDetails = string.Join("；", result.Warnings.Take(5).Select(warning =>
                $"{BackupService.GetModuleDisplayName(warning.Module)}：{ValueOrDash(warning.RelativePath)} — {warning.Error}"));
            OperationText = result.IsPartial
                ? $"恢复完成（有警告）：成功 {succeeded}；未恢复 {ValueOrDash(failed)}；共恢复 {result.RestoredFiles:N0} 个文件，{result.Warnings.Count} 项警告。{warningDetails}。恢复前备份：{result.PreRestoreBackupPath}"
                : $"完整恢复成功：{succeeded}，共 {result.RestoredFiles:N0} 个文件。恢复前备份：{result.PreRestoreBackupPath}";
            AddRecent(OperationText);
        }
        catch (Exception exception) { OperationText = UiText.UserError(exception, "恢复备份"); _logs.AddException("backup", "完整恢复失败。", exception); }
        finally { Busy = false; }
    }

    private IProgress<BackupProgress> CreateProgress() => new Progress<BackupProgress>(value =>
    {
        ProgressStage = $"{value.Stage} · {value.ProcessedFiles:N0}/{value.TotalFiles:N0} 个文件 · {FormatSize(value.ProcessedBytes)}/{FormatSize(value.TotalBytes)}";
        ProgressPercent = value.Percent;
    });
    private void AddRecent(string text) { RecentOperations.Insert(0, $"{DateTime.Now:HH:mm:ss}  {text}"); while (RecentOperations.Count > 10) RecentOperations.RemoveAt(RecentOperations.Count - 1); }
    private static string BuildBackupDetails(BackupManifest manifest)
    {
        var status = manifest.Status switch
        {
            BackupStatuses.Complete => "可恢复，完整",
            BackupStatuses.CompleteWithWarnings => "可恢复，有警告",
            _ => manifest.CanRestore ? "可部分恢复" : "无法恢复"
        };
        var modules = manifest.Modules.Count == 0
            ? "—"
            : string.Join("\n", manifest.Modules.Select(module =>
                $"- {module.DisplayName}：{(module.Status == "Complete" ? "完整" : module.CanRestore ? "可部分恢复" : "不可用")}（{module.ValidFileCount} 个有效文件）"));
        var missing = manifest.Failures.Concat(manifest.ValidationIssues.Select(issue => new BackupFailure
            {
                RelativePath = issue.RelativePath,
                Module = issue.Module,
                Error = issue.Error
            })).ToList();
        var missingText = missing.Count == 0
            ? "无"
            : string.Join("\n", missing.Take(20).Select(item => $"- {BackupService.GetModuleDisplayName(item.Module)}：{item.RelativePath} — {item.Error}")) +
              (missing.Count > 20 ? $"\n- 另有 {missing.Count - 20} 项，详见运行日志" : "");
        return $"备份时间  {manifest.CreatedAt.ToLocalTime():yyyy-MM-dd HH:mm}\n" +
               $"应用版本  {manifest.AppVersion}\n" +
               $"备份格式  v{manifest.FormatVersion}\n" +
               $"Codex 版本  {ValueOrDash(manifest.CodexVersion)}\n" +
               $"总体状态  {status}\n" +
               $"是否可恢复  {(manifest.CanRestore ? "是" : "否")}\n" +
               $"文件数量  {manifest.FileCount:N0}\n" +
               $"总大小  {FormatSize(manifest.TotalSize)}\n" +
               $"已排除运行时数据  {manifest.ExcludedRuntimeFiles.Count:N0} 项\n\n" +
               $"可恢复数据项\n{modules}\n\n" +
               $"缺失或无效项\n{missingText}";
    }
    private static string ValueOrDash(string value) => string.IsNullOrWhiteSpace(value) ? "—" : value;
    public static string FormatSize(long bytes) => bytes switch { >= 1L << 30 => $"{bytes / (double)(1L << 30):0.##} GB", >= 1L << 20 => $"{bytes / (double)(1L << 20):0.##} MB", >= 1L << 10 => $"{bytes / 1024d:0.##} KB", _ => $"{bytes:N0} B" };
    private static void OpenDirectory(string path) { Directory.CreateDirectory(path); Process.Start(new ProcessStartInfo("explorer.exe", path) { UseShellExecute = true }); }
}
