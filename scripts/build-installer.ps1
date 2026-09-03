param(
    [string]$Version = "1.2.3",
    [string]$OutputDirectory = "",
    [string]$InnoCompiler = ""
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$artifactsRoot = Join-Path $projectRoot "artifacts"
$releaseDirectory = if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    Join-Path $artifactsRoot "win-x64-$Version"
} else {
    $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputDirectory)
}
$stageDirectory = Join-Path $artifactsRoot ".installer-stage\$Version\win-x64"
$installerScript = Join-Path $projectRoot "installer\CloudLight.CodexBridge.iss"
$iconFile = Join-Path $projectRoot "apps\desktop\CloudLight.CodexBridge\Resources\AppIcon.ico"

if ([string]::IsNullOrWhiteSpace($InnoCompiler)) {
    $compilerCandidates = @(
        "C:\Program Files\Inno Setup 7\ISCC.exe",
        "C:\Program Files (x86)\Inno Setup 7\ISCC.exe",
        "C:\Program Files\Inno Setup 6\ISCC.exe",
        "C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
        (Join-Path $env:LOCALAPPDATA "Programs\Inno Setup 7\ISCC.exe"),
        (Join-Path $env:LOCALAPPDATA "Programs\Inno Setup 6\ISCC.exe")
    )
    $InnoCompiler = $compilerCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
}
if ([string]::IsNullOrWhiteSpace($InnoCompiler) -or -not (Test-Path -LiteralPath $InnoCompiler)) {
    throw "Inno Setup compiler was not found. Install Inno Setup 6 or 7, or pass -InnoCompiler."
}

$resolvedArtifactsRoot = [System.IO.Path]::GetFullPath($artifactsRoot).TrimEnd('\') + '\'
$resolvedStageDirectory = [System.IO.Path]::GetFullPath($stageDirectory)
if (-not $resolvedStageDirectory.StartsWith($resolvedArtifactsRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to clean installer stage outside the artifacts directory: $resolvedStageDirectory"
}
if (Test-Path -LiteralPath $resolvedStageDirectory) {
    Remove-Item -LiteralPath $resolvedStageDirectory -Recurse -Force
}

& (Join-Path $PSScriptRoot "build.ps1") `
    -Version $Version `
    -OutputDirectory $resolvedStageDirectory `
    -SelfContained `
    -ExcludeSymbols
if ($LASTEXITCODE -ne 0) { throw "Self-contained application publish failed." }

New-Item -ItemType Directory -Path $releaseDirectory -Force | Out-Null
$defineVersion = '/DMyAppVersion=' + $Version
$defineSource = '/DSourceDir=' + $resolvedStageDirectory
$defineOutput = '/DOutputDir=' + $releaseDirectory
$defineIcon = '/DAppIconFile=' + $iconFile
$compilerArguments = @($defineVersion, $defineSource, $defineOutput, $defineIcon, $installerScript)
# Use PowerShell's native argument passing so the release script works in both
# Windows PowerShell 5.1 and PowerShell 7.  ProcessStartInfo.ArgumentList is
# unavailable in the former, and every path here may contain spaces.
& $InnoCompiler @compilerArguments
if ($LASTEXITCODE -ne 0) { throw "Inno Setup compilation failed." }

$installerPath = Join-Path $releaseDirectory "CloudLight-CodexBridge-Setup-$Version-win-x64.exe"
if (-not (Test-Path -LiteralPath $installerPath)) {
    throw "Installer compiler completed without producing the expected file: $installerPath"
}

$installer = Get-Item -LiteralPath $installerPath
$hash = Get-FileHash -LiteralPath $installerPath -Algorithm SHA256
$checksumPath = "$installerPath.sha256"
Set-Content -LiteralPath $checksumPath -Encoding ascii -Value ("{0} *{1}" -f $hash.Hash.ToLowerInvariant(), $installer.Name)

$stageRoot = Split-Path -Parent $resolvedStageDirectory
if (Test-Path -LiteralPath $stageRoot) {
    Remove-Item -LiteralPath $stageRoot -Recurse -Force
}

Write-Output "Installer completed: $installerPath"
Write-Output "Size: $($installer.Length) bytes"
Write-Output "SHA-256: $($hash.Hash)"
