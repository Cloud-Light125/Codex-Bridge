using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using CloudLight.CodexBridge.Models;

namespace CloudLight.CodexBridge.Services;

public sealed class DaemonProcessManager(LogService logs) : IAsyncDisposable
{
    private Process? _process;
    private string _lastStderr = "";

    public Uri? Address { get; private set; }
    public string Token { get; private set; } = "";
    public int ProcessId => _process?.HasExited == false ? _process.Id : 0;

    public async Task<ReadyMessage> StartAsync(UserSettings settings, CancellationToken cancellationToken = default)
        => await StartAsync(settings, null, cancellationToken);

    public async Task<ReadyMessage> StartAsync(
        UserSettings settings,
        string? codexPathOverride,
        CancellationToken cancellationToken = default)
    {
        if (_process is { HasExited: false })
        {
            throw new InvalidOperationException("后端进程已经在运行。");
        }

        var executable = LocateDaemon();
        Token = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32))
            .TrimEnd('=').Replace('+', '-').Replace('/', '_');

        var startInfo = new ProcessStartInfo
        {
            FileName = executable,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            RedirectStandardInput = true,
            CreateNoWindow = true,
            WorkingDirectory = AppContext.BaseDirectory
        };
        startInfo.ArgumentList.Add("--listen");
        startInfo.ArgumentList.Add("127.0.0.1:0");
        startInfo.ArgumentList.Add("--token");
        startInfo.ArgumentList.Add(Token);
        startInfo.ArgumentList.Add("--sandbox");
        startInfo.ArgumentList.Add(settings.SandboxMode is "read-only" ? "read-only" : "workspace-write");
        var effectiveCodexPath = codexPathOverride is not null
            ? codexPathOverride.Trim()
            : CodexPathSettings.EffectiveSavedPath(settings);
        if (!string.IsNullOrWhiteSpace(effectiveCodexPath))
        {
            startInfo.ArgumentList.Add("--codex-path");
            startInfo.ArgumentList.Add(effectiveCodexPath);
        }

        var process = new Process { StartInfo = startInfo, EnableRaisingEvents = true };
        _process = process;
        process.Exited += (sender, _) =>
        {
            if (sender is Process exitedProcess)
            {
                logs.Add("desktop", $"后端进程已退出，退出码 {exitedProcess.ExitCode}。");
            }
        };
        try
        {
            if (!process.Start())
            {
                throw new InvalidOperationException("操作系统未能启动 bridge-daemon.exe。");
            }
        }
        catch (Exception exception)
        {
            throw new InvalidOperationException($"启动后端失败：{exception.Message}", exception);
        }

        _ = ReadStderrAsync(process, cancellationToken);
        string? readyLine;
        try
        {
            readyLine = await process.StandardOutput.ReadLineAsync(cancellationToken)
                .AsTask().WaitAsync(TimeSpan.FromSeconds(20), cancellationToken);
        }
        catch (TimeoutException exception)
        {
            await StopAsync();
            throw new InvalidOperationException($"等待后端 ready 消息超时。{_lastStderr}", exception);
        }

        if (string.IsNullOrWhiteSpace(readyLine))
        {
            var exit = process.HasExited ? $"退出码 {process.ExitCode}" : "未输出内容";
            await StopAsync();
            throw new InvalidOperationException($"后端未返回 ready JSON（{exit}）。{_lastStderr}");
        }

        ReadyMessage ready;
        try
        {
            ready = JsonSerializer.Deserialize<ReadyMessage>(readyLine) ?? throw new JsonException("ready JSON 为空");
        }
        catch (JsonException exception)
        {
            await StopAsync();
            throw new InvalidOperationException($"后端 ready JSON 无法解析：{LogService.Redact(exception.Message)}", exception);
        }

        if (ready.Type != "ready" || ready.Token != Token || !Uri.TryCreate(ready.Address, UriKind.Absolute, out var address) ||
            address.Scheme != Uri.UriSchemeHttp || address.Host != "127.0.0.1")
        {
            await StopAsync();
            throw new InvalidOperationException("后端 ready 消息未通过本地地址或令牌校验。");
        }

        Address = address;
        logs.Add("desktop", $"后端已启动（PID {ready.Pid}，{address}）。");
        _ = DrainStdoutAsync(process, cancellationToken);
        return ready;
    }

    private static string LocateDaemon()
    {
        var candidates = new[]
        {
            Path.Combine(AppContext.BaseDirectory, "bridge-daemon.exe"),
            Path.Combine(AppContext.BaseDirectory, "daemon", "bridge-daemon.exe")
        };
        var found = candidates.FirstOrDefault(File.Exists);
        if (found is null)
        {
            throw new FileNotFoundException(
                "未找到 bridge-daemon.exe。请先运行 scripts/dev.ps1 或 scripts/build.ps1。",
                candidates[0]);
        }
        return found;
    }

    private async Task ReadStderrAsync(Process process, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var line = await process.StandardError.ReadLineAsync(cancellationToken);
                if (line is null) break;
                _lastStderr = LogService.Redact(line);
                logs.Add("daemon", line);
            }
        }
        catch (OperationCanceledException) { }
        catch (InvalidOperationException) { }
    }

    private static async Task DrainStdoutAsync(Process process, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (await process.StandardOutput.ReadLineAsync(cancellationToken) is null) break;
            }
        }
        catch (OperationCanceledException) { }
        catch (InvalidOperationException) { }
    }

    public async Task StopAsync()
    {
        var process = _process;
        _process = null;
        if (process is null) return;
        try
        {
            if (!process.HasExited)
            {
                process.StandardInput.Close();
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(7));
                try
                {
                    await process.WaitForExitAsync(timeout.Token);
                }
                catch (OperationCanceledException)
                {
                    logs.Add("desktop", "后端未在超时内退出，正在终止由桌面端启动的进程树。");
                    process.Kill(entireProcessTree: true);
                    await process.WaitForExitAsync();
                }
            }
        }
        finally
        {
            process.Dispose();
            Address = null;
            Token = "";
        }
    }

    public async ValueTask DisposeAsync() => await StopAsync();
}
