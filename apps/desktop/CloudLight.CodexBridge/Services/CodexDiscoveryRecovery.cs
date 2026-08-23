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
        if (!discovery.Found || discovery.Source == CodexDiscoverySource.SavedPath ||
            string.Equals(settings.DetectedCodexPath, discovery.Path, StringComparison.OrdinalIgnoreCase))
            return false;

        settings.DetectedCodexPath = discovery.Path;
        return true;
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
