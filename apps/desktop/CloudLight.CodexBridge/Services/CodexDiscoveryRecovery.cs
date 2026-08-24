using CloudLight.CodexBridge.Models;

namespace CloudLight.CodexBridge.Services;

public static class CodexPathSettings
{
    public static string EffectiveSavedPath(UserSettings settings) =>
        !string.IsNullOrWhiteSpace(settings.CodexCustomPath)
            ? settings.CodexCustomPath.Trim()
            : settings.DetectedCodexPath.Trim();

    public static bool RememberAutomaticDiscovery(UserSettings settings, CodexDiscoveryResult discovery)
    {
        if (!discovery.Found || discovery.Source is not (
                CodexDiscoverySource.PATH or CodexDiscoverySource.CodexProcess or CodexDiscoverySource.ChatGPTProcess) ||
            string.Equals(settings.DetectedCodexPath, discovery.Path, StringComparison.OrdinalIgnoreCase))
            return false;

        settings.DetectedCodexPath = discovery.Path;
        return true;
    }

    public static bool TryPrepareManualSave(
        UserSettings settings,
        string? requestedPath,
        CodexDiscoveryResult validation,
        out string pathToApply)
    {
        var requested = requestedPath?.Trim() ?? "";
        if (requested.Length == 0)
        {
            settings.CodexCustomPath = "";
            pathToApply = "";
            return true;
        }

        if (!validation.Found || validation.Source != CodexDiscoverySource.Manual ||
            string.IsNullOrWhiteSpace(validation.Path))
        {
            pathToApply = "";
            return false;
        }

        settings.CodexCustomPath = validation.Path;
        pathToApply = validation.Path;
        return true;
    }

    public static void RemoveUnsafeAutomaticPath(UserSettings settings)
    {
        if (CodexDiscoveryService.IsPackagedAppInternalPath(settings.DetectedCodexPath))
            settings.DetectedCodexPath = "";
    }
}

public sealed class CodexDiscoveryRetryRunner(
    LogService logs,
    IReadOnlyList<TimeSpan>? delays = null)
{
    private readonly IReadOnlyList<TimeSpan> _delays = delays ??
        [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(8), TimeSpan.FromSeconds(15)];

    public async Task<bool> RunAsync(
        Func<CancellationToken, Task<CodexDiscoveryResult>> discoverAsync,
        Func<CodexDiscoveryResult, CancellationToken, Task> recoveredAsync,
        CancellationToken cancellationToken)
    {
        if (_delays.Count == 0) throw new InvalidOperationException("At least one Codex discovery retry delay is required.");
        for (var index = 0; ; index++)
        {
            var delay = _delays[Math.Min(index, _delays.Count - 1)];
            if (index < _delays.Count || (index + 1) % 4 == 0)
                logs.Add("codex-discovery", $"[codex-discovery] retry scheduled attempt={index + 1} delayMs={(long)delay.TotalMilliseconds}");
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);

            try
            {
                var discovery = await discoverAsync(cancellationToken).ConfigureAwait(false);
                if (!discovery.Found) continue;
                await recoveredAsync(discovery, cancellationToken).ConfigureAwait(false);
                logs.Add("codex-discovery", $"[codex-discovery] retry recovered attempt={index + 1} source={discovery.RuntimeSource} path={discovery.Path}");
                return true;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                logs.AddException("codex-discovery", $"Codex 后台检测第 {index + 1} 次尝试未能应用。", exception);
            }
        }
    }
}
