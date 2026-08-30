using System.Collections.ObjectModel;
using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;

namespace CloudLight.CodexBridge.ViewModels;

public sealed class CommandItemViewModel(RemoteCommandDefinition model)
{
    public RemoteCommandDefinition Model { get; } = model;
    public string Id => Model.Id;
    public string Name => Model.Name;
    public string DisplayName => Model.DisplayName;
    public string Description => Model.Description;
    public string AliasesDisplay => Model.Aliases is { Count: > 0 } aliases ? string.Join("、", aliases) : "无";
    public string ChineseName => Model.DisplayName;
    public string UsageExample => Model.Name switch
    {
        "/bind" => "/bind 12",
        "/thread" => "/thread 12",
        "/history" => "/history 12 3",
        "/status" => "/status 12",
        "/stop" => "/stop 12",
        _ => Model.Name
    };
    public string ChannelSupportDisplay => "支持 QQ / Telegram";
    public string TypeDisplay => Model.BuiltIn ? "系统指令" : "自定义指令";
    public string CapabilityDisplay => Model.BackendCapability switch
    {
        "openclaw" => "OpenClaw 专属",
        "both" => "Codex + OpenClaw",
        _ => "Codex 系统"
    };
    public string CompatibilityDisplay => Model.BackendCapability switch
    {
        "both" => "兼容 Codex 同名指令：是",
        "openclaw" => "兼容 Codex 同名指令：否",
        _ => "兼容 OpenClaw 同名指令：否"
    };
    public string StatusDisplay => Model.Enabled ? "已启用" : "已停用";
    public string LockDisplay => Model.BuiltIn ? (Model.Locked ? "🔒 已锁定" : "🔓 已解锁") : "";
    public Visibility LockVisibility => Model.BuiltIn ? Visibility.Visible : Visibility.Collapsed;
    public Visibility UnlockVisibility => Model.BuiltIn && Model.Locked ? Visibility.Visible : Visibility.Collapsed;
    public Visibility LockButtonVisibility => Model.BuiltIn && !Model.Locked ? Visibility.Visible : Visibility.Collapsed;
    public Visibility EditVisibility => !Model.BuiltIn || !Model.Locked ? Visibility.Visible : Visibility.Collapsed;
    public Visibility DeleteVisibility => Model.CanDelete ? Visibility.Visible : Visibility.Collapsed;
    public Visibility TelegramNoticeVisibility => string.IsNullOrWhiteSpace(Model.TelegramMenuNotice) ? Visibility.Collapsed : Visibility.Visible;
}

public sealed class CommandsViewModel : ObservableObject
{
    private readonly BridgeApiClient _api;
    private readonly LogService _logs;
    private readonly string _backend;
    private bool _initialized;
    private bool _editorOpen;
    private bool _editingBuiltIn;
    private string _editingId = "";
    private string _editorTitle = "添加指令";
    private string _name = "";
    private string _displayName = "";
    private string _aliasesText = "";
    private string _description = "";
    private string _parameterHelp = "";
    private string _selectedAction = "";
    private bool _enabled = true;
    private string _operationMessage = "";

    public CommandsViewModel(BridgeApiClient api, LogService logs, string backend = "codex")
    {
        _api = api;
        _logs = logs;
        _backend = string.Equals(backend, "openclaw", StringComparison.OrdinalIgnoreCase) ? "openclaw" : "codex";
        RefreshCommand = new AsyncRelayCommand(() => RefreshAsync());
        AddCommand = new RelayCommand(_ => BeginAdd());
        EditCommand = new RelayCommand(value => BeginEdit(value as CommandItemViewModel));
        CancelEditCommand = new RelayCommand(_ => IsEditorOpen = false);
        SaveCommand = new AsyncRelayCommand(SaveAsync);
        UnlockCommand = new RelayCommand(value => _ = SetLockedAsync(value as CommandItemViewModel, false));
        LockCommand = new RelayCommand(value => _ = SetLockedAsync(value as CommandItemViewModel, true));
        RestoreCommand = new RelayCommand(value => _ = RestoreAsync(value as CommandItemViewModel));
        DeleteCommand = new RelayCommand(value => _ = DeleteAsync(value as CommandItemViewModel));
    }

    public ObservableCollection<CommandItemViewModel> Commands { get; } = [];
    public ObservableCollection<RemoteCommandAction> Actions { get; } = [];
    public bool IsOpenClaw => _backend == "openclaw";
    public string PageTitle => IsOpenClaw ? "OpenClaw 指令" : "Codex 指令";
    public string PageDescription => IsOpenClaw
        ? "只显示可在 OpenClaw 路由中使用的远程指令；运行状态和 Session 由 OpenClaw Gateway 提供。"
        : "只显示 Codex 路由中可用的远程指令；Codex Thread、运行状态和额度能力保持独立。";
    public string BackendNotice => IsOpenClaw ? "当前聊天路由到 OpenClaw 时，序号表示 OpenClaw Session；路由到 Codex 时，序号表示 Codex Thread。" : "";
    public Visibility BackendNoticeVisibility => IsOpenClaw ? Visibility.Visible : Visibility.Collapsed;
    public ICommand RefreshCommand { get; }
    public ICommand AddCommand { get; }
    public ICommand EditCommand { get; }
    public ICommand CancelEditCommand { get; }
    public ICommand SaveCommand { get; }
    public ICommand UnlockCommand { get; }
    public ICommand LockCommand { get; }
    public ICommand RestoreCommand { get; }
    public ICommand DeleteCommand { get; }

    public bool IsEditorOpen { get => _editorOpen; set { if (SetProperty(ref _editorOpen, value)) OnPropertyChanged(nameof(EditorVisibility)); } }
    public Visibility EditorVisibility => IsEditorOpen ? Visibility.Visible : Visibility.Collapsed;
    public bool IsActionEditable => !_editingBuiltIn;
    public string EditorTitle { get => _editorTitle; private set => SetProperty(ref _editorTitle, value); }
    public string Name { get => _name; set => SetProperty(ref _name, value); }
    public string DisplayName { get => _displayName; set => SetProperty(ref _displayName, value); }
    public string AliasesText { get => _aliasesText; set => SetProperty(ref _aliasesText, value); }
    public string Description { get => _description; set => SetProperty(ref _description, value); }
    public string ParameterHelp { get => _parameterHelp; set => SetProperty(ref _parameterHelp, value); }
    public string SelectedAction { get => _selectedAction; set => SetProperty(ref _selectedAction, value); }
    public bool Enabled { get => _enabled; set => SetProperty(ref _enabled, value); }
    public string OperationMessage { get => _operationMessage; private set => SetProperty(ref _operationMessage, value); }

    public async Task EnsureInitializedAsync(CancellationToken cancellationToken = default)
    {
        if (_initialized) return;
        await RefreshAsync(cancellationToken);
    }

    public async Task RefreshAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _api.GetCommandsForBackendAsync(_backend, cancellationToken);
            Commands.Clear();
            foreach (var item in response.Commands)
            {
                item.Aliases ??= [];
                Commands.Add(new CommandItemViewModel(item));
            }
            Actions.Clear();
            foreach (var action in response.Actions) Actions.Add(action);
            _initialized = true;
            OperationMessage = $"已加载 {Commands.Count} 条{PageTitle}；QQ 与 Telegram 共用此配置。";
        }
        catch (Exception exception)
        {
            OperationMessage = UiText.UserError(exception, "读取指令");
            _logs.AddException("desktop", "读取远程指令失败。", exception);
        }
    }

    private void BeginAdd()
    {
        _editingId = "";
        _editingBuiltIn = false;
        EditorTitle = "添加指令";
        Name = "/";
        DisplayName = "";
        AliasesText = "";
        Description = "";
        ParameterHelp = "";
        SelectedAction = Actions.FirstOrDefault()?.Id ?? "";
        Enabled = true;
        OnPropertyChanged(nameof(IsActionEditable));
        IsEditorOpen = true;
    }

    private void BeginEdit(CommandItemViewModel? item)
    {
        if (item is null || item.Model.Locked) return;
        var model = item.Model;
        _editingId = model.Id;
        _editingBuiltIn = model.BuiltIn;
        EditorTitle = $"编辑 {model.Name}";
        Name = model.Name;
        DisplayName = model.DisplayName;
        AliasesText = string.Join(Environment.NewLine, model.Aliases ?? []);
        Description = model.Description;
        ParameterHelp = model.ParameterHelp;
        SelectedAction = model.Action;
        Enabled = model.Enabled;
        OnPropertyChanged(nameof(IsActionEditable));
        IsEditorOpen = true;
    }

    private async Task SaveAsync()
    {
        try
        {
            var input = new RemoteCommandMutation
            {
                Name = Name.Trim(), DisplayName = DisplayName.Trim(), Description = Description.Trim(),
                ParameterHelp = ParameterHelp.Trim(), Action = SelectedAction, Enabled = Enabled,
                Aliases = AliasesText.Split([',', '，', '\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Distinct(StringComparer.OrdinalIgnoreCase).ToList()
            };
            if (string.IsNullOrWhiteSpace(_editingId)) await _api.CreateCommandAsync(input);
            else await _api.UpdateCommandAsync(_editingId, input);
            IsEditorOpen = false;
            await RefreshAsync();
            OperationMessage = $"指令已保存，{PageTitle} 页面已刷新。有效配置立即同时用于 QQ 和 Telegram。";
        }
        catch (Exception exception)
        {
            OperationMessage = UiText.UserError(exception, "保存指令");
            _logs.AddException("desktop", "保存远程指令失败。", exception);
        }
    }

    private async Task SetLockedAsync(CommandItemViewModel? item, bool locked)
    {
        if (item is null) return;
        try
        {
            await _api.SetCommandLockedAsync(item.Id, locked);
            if (locked && _editingId == item.Id) IsEditorOpen = false;
            await RefreshAsync();
            OperationMessage = locked ? $"{item.Name} 已重新锁定。" : $"{item.Name} 已解锁，可以编辑。";
        }
        catch (Exception exception) { ShowOperationError(exception, locked ? "锁定指令" : "解锁指令"); }
    }

    private async Task RestoreAsync(CommandItemViewModel? item)
    {
        if (item is null) return;
        try
        {
            await _api.RestoreCommandAsync(item.Id);
            if (_editingId == item.Id) IsEditorOpen = false;
            await RefreshAsync();
            OperationMessage = $"仅 {item.Name} 已恢复初始指令，其他指令未更改。";
        }
        catch (Exception exception) { ShowOperationError(exception, "恢复初始指令"); }
    }

    private async Task DeleteAsync(CommandItemViewModel? item)
    {
        if (item is null || !item.Model.CanDelete) return;
        if (MessageBox.Show($"确定删除“{item.Name}”？", "删除指令", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes) return;
        try
        {
            await _api.DeleteCommandAsync(item.Id);
            if (_editingId == item.Id) IsEditorOpen = false;
            await RefreshAsync();
            OperationMessage = $"已删除 {item.Name}。";
        }
        catch (Exception exception) { ShowOperationError(exception, "删除指令"); }
    }

    private void ShowOperationError(Exception exception, string operation)
    {
        OperationMessage = UiText.UserError(exception, operation);
        _logs.AddException("desktop", operation + "失败。", exception);
    }
}
