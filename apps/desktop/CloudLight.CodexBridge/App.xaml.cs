using System.ComponentModel;
using System.Drawing;
using System.Windows.Media;
using System.Windows.Input;
using System.Windows.Threading;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;
using CloudLight.CodexBridge.ViewModels;
using Microsoft.Win32;
using Forms = System.Windows.Forms;

namespace CloudLight.CodexBridge;

public partial class App : Application
{
    public static bool IsDarkTheme { get; private set; }
    private MainViewModel? _mainViewModel;
    private SettingsService? _settingsService;
    private UserSettings? _settings;
    private LogService? _logs;
    private Forms.NotifyIcon? _trayIcon;
    private Forms.ToolStripMenuItem? _trayStatusItem;
    private DispatcherTimer? _refreshTimer;
    private bool _shutdownInProgress;
    private bool _exitRequested;
    private bool _trayNoticeShown;
    private int _showMainWindowPending;

    protected override async void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);
        EventManager.RegisterClassHandler(typeof(TextBox), UIElement.PreviewMouseRightButtonDownEvent,
            new MouseButtonEventHandler(OnReadOnlyTextBoxRightClick));
        var silent = e.Args.Any(argument => argument.Equals("--silent", StringComparison.OrdinalIgnoreCase));
        var logs = new LogService();
        _logs = logs;
        InstallExceptionHandlers(silent);
        var settingsService = new SettingsService();
        _settingsService = settingsService;
        var settings = await settingsService.LoadAsync();
        _settings = settings;
        ApplyTheme(settings.Theme);
        if (!string.IsNullOrWhiteSpace(settingsService.LastLoadWarning)) logs.Add("desktop", settingsService.LastLoadWarning);

        var codexDiscoveryService = new CodexDiscoveryService(logs);
        var codexDiscovery = await codexDiscoveryService.DiscoverAsync(
            settings.CodexCustomPath, settings.DetectedCodexPath);
        if (CodexPathSettings.RememberAutomaticDiscovery(settings, codexDiscovery))
        {
            try
            {
                await settingsService.SaveAsync(settings);
                logs.Add("codex-config", $"[codex-config] persisted detected path={settings.DetectedCodexPath}");
            }
            catch (Exception exception) { logs.AddException("desktop", "保存自动发现的 Codex 路径失败。", exception); }
        }

        var daemon = new DaemonProcessManager(logs);
        var api = new BridgeApiClient(logs);
        var sessions = new SessionsViewModel(api, logs);
        var commands = new CommandsViewModel(api, logs);
        var channels = new ChannelsViewModel(api, settingsService, new TelegramSecretService(), new QqSecretService(), settings, logs);
        var startup = new StartupService();
        var settingsViewModel = new SettingsViewModel(settingsService, api, settings, logs, startup, codexDiscoveryService);
        settingsViewModel.UpdateDiscovery(codexDiscovery);
        var logsViewModel = new LogsViewModel(logs, settingsService.LogDirectory);
        var overview = new OverviewViewModel(sessions, channels, settingsViewModel);
        var mirror = new MirrorViewModel(settingsViewModel);
        var backup = new BackupViewModel(new BackupService(settingsService, logs: logs), logs);
        _mainViewModel = new MainViewModel(daemon, api, sessions, channels, commands, overview, mirror, backup,
            settingsViewModel, logsViewModel, settings, settingsService, logs, codexDiscoveryService, codexDiscovery);
        backup.StopRuntimeAsync = _mainViewModel.PauseRuntimeAsync;
        backup.RestartRuntimeAsync = _mainViewModel.ResumeRuntimeAsync;
        backup.RefreshThreadsAsync = () => sessions.RefreshAsync();

        var window = new MainWindow { DataContext = _mainViewModel };
        RestoreWindow(window, settings);
        window.Closing += OnMainWindowClosing;
        window.StateChanged += (_, _) => SaveWindowSnapshot(window);
        window.LocationChanged += (_, _) => SaveWindowSnapshot(window);
        window.SizeChanged += (_, _) => SaveWindowSnapshot(window);
        MainWindow = window;
        CreateTrayIcon();
        if (!silent) window.Show();
        await _mainViewModel.InitializeAsync();
        UpdateTrayStatus();

        _refreshTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(Math.Clamp(settings.ThreadRefreshIntervalSeconds, 10, 300)) };
        _refreshTimer.Tick += async (_, _) =>
        {
            if (_settings?.AutoRefreshThreads != true || _mainViewModel is null) return;
            _refreshTimer.Interval = TimeSpan.FromSeconds(Math.Clamp(_settings.ThreadRefreshIntervalSeconds, 10, 300));
            await _mainViewModel.RefreshAsync();
            UpdateTrayStatus();
        };
        _refreshTimer.Start();
    }

    private static void OnReadOnlyTextBoxRightClick(object sender, MouseButtonEventArgs e)
    {
        if (sender is not TextBox { IsReadOnly: true } textBox) return;
        textBox.Focus();
        var menu = new ContextMenu { PlacementTarget = textBox };
        var copy = new MenuItem { Header = "复制", IsEnabled = !string.IsNullOrEmpty(textBox.SelectedText) };
        copy.Click += (_, _) => textBox.Copy();
        var selectAll = new MenuItem { Header = "全选" };
        selectAll.Click += (_, _) => textBox.SelectAll();
        menu.Items.Add(copy);
        menu.Items.Add(new Separator());
        menu.Items.Add(selectAll);
        textBox.ContextMenu = menu;
        menu.IsOpen = true;
        e.Handled = true;
    }

    public static void ApplyTheme(string? requested)
    {
        if (Current is null) return;
        var theme = requested;
        if (theme == "system")
        {
            try
            {
                using var key = Registry.CurrentUser.OpenSubKey(@"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize");
                theme = Convert.ToInt32(key?.GetValue("AppsUseLightTheme", 1)) == 0 ? "dark" : "light";
            }
            catch { theme = "light"; }
        }
        var dark = theme == "dark";
        IsDarkTheme = dark;
        SetBrush("AccentBrush", dark ? "#60A5FA" : "#2563EB");
        SetBrush("AccentHoverBrush", dark ? "#93C5FD" : "#1D4ED8");
        SetBrush("BackgroundBrush", dark ? "#11151C" : "#F5F7FB");
        SetBrush("WindowBrush", dark ? "#11151C" : "#F5F7FB");
        SetBrush("SurfaceBrush", dark ? "#1B2029" : "#FFFFFF");
        SetBrush("CardBrush", dark ? "#1B2029" : "#FFFFFF");
        SetBrush("SidebarBrush", dark ? "#161B23" : "#F9FAFC");
        SetBrush("BorderBrush", dark ? "#303846" : "#E2E7EF");
        SetBrush("PrimaryTextBrush", dark ? "#EEF2F8" : "#172033");
        SetBrush("TextBrush", dark ? "#EEF2F8" : "#172033");
        SetBrush("SecondaryTextBrush", dark ? "#A8B1C0" : "#667085");
        SetBrush("MutedBrush", dark ? "#A8B1C0" : "#667085");
        SetBrush("DisabledTextBrush", dark ? "#6F7A8B" : "#98A2B3");
        SetBrush("SubtleAccentBrush", dark ? "#20345C" : "#EAF1FF");
        SetBrush("CodeBrush", dark ? "#121720" : "#F3F5F8");
        SetBrush("ScrollTrackBrush", dark ? "#171C24" : "#EEF1F5");
        SetBrush("ScrollThumbBrush", dark ? "#4A5566" : "#AAB4C2");
        SetBrush("WarningSurfaceBrush", dark ? "#352A18" : "#FFF4D6");
        SetBrush("SelectionBrush", dark ? "#375A97" : "#7EA7F8");
        SetBrush("SuccessBrush", dark ? "#32D583" : "#16835B");
        SetBrush("WarningBrush", dark ? "#FDB022" : "#A15C00");
        SetBrush("ErrorBrush", dark ? "#F97066" : "#B42318");
        if (Current.MainWindow is MainWindow mainWindow) mainWindow.ApplyTitleBarTheme(dark);
    }

    private static void SetBrush(string key, string color)
    {
        var parsedColor = (System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString(color);
        if (Current.Resources[key] is SolidColorBrush brush && !brush.IsFrozen)
        {
            brush.Color = parsedColor;
            return;
        }

        Current.Resources[key] = new SolidColorBrush(parsedColor);
    }

    private void CreateTrayIcon()
    {
        var menu = new Forms.ContextMenuStrip();
        menu.Items.Add("打开 CloudLight Codex Bridge", null, (_, _) => ShowMainWindow());
        _trayStatusItem = new Forms.ToolStripMenuItem("状态：正在启动") { Enabled = false };
        menu.Items.Add(_trayStatusItem);
        menu.Items.Add(new Forms.ToolStripSeparator());
        menu.Items.Add("设置", null, (_, _) => { ShowMainWindow(); _mainViewModel?.NavigateCommand.Execute("settings"); });
        menu.Items.Add("退出", null, async (_, _) => await ExitApplicationAsync());
        Icon icon;
        var resource = GetResourceStream(new Uri("pack://application:,,,/Resources/AppIcon.ico"));
        if (resource is not null)
        {
            using (resource.Stream) using (var loaded = new Icon(resource.Stream)) icon = (Icon)loaded.Clone();
        }
        else icon = SystemIcons.Application;
        _trayIcon = new Forms.NotifyIcon { Text = "CloudLight Codex Bridge", Icon = icon, Visible = true, ContextMenuStrip = menu };
        _trayIcon.MouseClick += (_, args) =>
        {
            if (args.Button == Forms.MouseButtons.Left) ShowMainWindow();
        };
    }

    private void UpdateTrayStatus()
    {
        if (_trayStatusItem is not null && _mainViewModel is not null) _trayStatusItem.Text = $"状态：{_mainViewModel.BackendState}";
    }

    private void ShowMainWindow()
    {
        if (_shutdownInProgress || _exitRequested || MainWindow is null) return;
        if (System.Threading.Interlocked.Exchange(ref _showMainWindowPending, 1) != 0) return;

        try
        {
            _ = Dispatcher.BeginInvoke(DispatcherPriority.Normal, new Action(() =>
            {
                try
                {
                    if (_shutdownInProgress || _exitRequested || MainWindow is null) return;

                    var window = MainWindow;
                    if (!window.IsVisible) window.Show();
                    if (window.WindowState == WindowState.Minimized) window.WindowState = WindowState.Normal;
                    window.Activate();
                    window.Focus();
                }
                finally
                {
                    System.Threading.Interlocked.Exchange(ref _showMainWindowPending, 0);
                }
            }));
        }
        catch
        {
            System.Threading.Interlocked.Exchange(ref _showMainWindowPending, 0);
            throw;
        }
    }
    private async void OnMainWindowClosing(object? sender, CancelEventArgs e)
    {
        if (_exitRequested) return;
        if (_settings?.CloseToTray != false)
        {
            e.Cancel = true;
            if (sender is Window window) { SaveWindowSnapshot(window); window.Hide(); }
            if (!_trayNoticeShown && _trayIcon is not null)
            {
                _trayNoticeShown = true;
                _trayIcon.ShowBalloonTip(2500, "CloudLight Codex Bridge", "应用仍在后台运行，可从系统托盘重新打开或退出。", Forms.ToolTipIcon.Info);
            }
            return;
        }
        e.Cancel = true;
        await ExitApplicationAsync();
    }

    private async Task ExitApplicationAsync()
    {
        if (_shutdownInProgress) return;
        _shutdownInProgress = true;
        _exitRequested = true;
        _refreshTimer?.Stop();
        if (MainWindow is not null) SaveWindowSnapshot(MainWindow);
        try
        {
            if (_settingsService is not null && _settings is not null) await _settingsService.SaveAsync(_settings);
            if (_mainViewModel is not null) await _mainViewModel.StopAsync();
        }
        catch (Exception exception) { _logs?.AddException("desktop", "关闭应用时发生异常。", exception); }
        finally
        {
            if (_trayIcon is not null) { _trayIcon.Visible = false; _trayIcon.Dispose(); _trayIcon = null; }
            Shutdown();
        }
    }

    private void SaveWindowSnapshot(Window window)
    {
        if (_settings is null || window.WindowState == WindowState.Minimized) return;
        var bounds = window.RestoreBounds;
        _settings.WindowWidth = bounds.Width;
        _settings.WindowHeight = bounds.Height;
        _settings.WindowLeft = bounds.Left;
        _settings.WindowTop = bounds.Top;
        _settings.WindowMaximized = window.WindowState == WindowState.Maximized;
    }

    private static void RestoreWindow(Window window, UserSettings settings)
    {
        window.Width = settings.WindowWidth;
        window.Height = settings.WindowHeight;
        if (double.IsFinite(settings.WindowLeft) && double.IsFinite(settings.WindowTop))
        {
            var proposed = new System.Drawing.Rectangle((int)settings.WindowLeft, (int)settings.WindowTop, (int)settings.WindowWidth, (int)settings.WindowHeight);
            var visible = Forms.Screen.AllScreens.Any(screen => screen.WorkingArea.IntersectsWith(proposed));
            if (visible) { window.WindowStartupLocation = WindowStartupLocation.Manual; window.Left = settings.WindowLeft; window.Top = settings.WindowTop; }
        }
        if (settings.WindowMaximized) window.WindowState = WindowState.Maximized;
    }

    private void InstallExceptionHandlers(bool silent)
    {
        DispatcherUnhandledException += (_, args) =>
        {
            _logs?.AddException("desktop", "UI Dispatcher 未处理异常。", args.Exception);
            if (_mainViewModel is not null && IsRecoverable(args.Exception)) { _mainViewModel.ReportRecoverableUiException(args.Exception); args.Handled = true; }
            else if (silent) args.Handled = true;
        };
        AppDomain.CurrentDomain.UnhandledException += (_, args) => { if (args.ExceptionObject is Exception exception) _logs?.AddException("desktop", "AppDomain 未处理异常。", exception); };
        TaskScheduler.UnobservedTaskException += (_, args) => { _logs?.AddException("desktop", "未观察的 Task 异常。", args.Exception); if (args.Exception.Flatten().InnerExceptions.All(exception => IsRecoverable(exception) || exception is OperationCanceledException)) args.SetObserved(); };
    }
    private static bool IsRecoverable(Exception exception) => exception is BridgeApiException or TelegramSecretException or QqSecretException;
}
