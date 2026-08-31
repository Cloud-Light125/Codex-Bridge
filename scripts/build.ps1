param(
    [string]$Version = "1.2.1",
    [string]$OutputDirectory = "",
    [switch]$SelfContained,
    [switch]$ExcludeSymbols
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$daemonRoot = Join-Path $projectRoot "services\bridge-daemon"
$desktopProject = Join-Path $projectRoot "apps\desktop\CloudLight.CodexBridge\CloudLight.CodexBridge.csproj"
$artifactsRoot = Join-Path $projectRoot "artifacts"
$publishDirectory = if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    Join-Path $artifactsRoot "win-x64-$Version"
} else {
    $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputDirectory)
}
$daemonExecutable = Join-Path $artifactsRoot "bridge-daemon.exe"

# A release directory is versioned specifically so an already running release is
# never overwritten. Choose a new version or an explicit empty output directory.
if (Test-Path -LiteralPath $publishDirectory) {
    throw "Release output directory already exists and will not be overwritten: $publishDirectory"
}

$goCommand = Get-Command go -ErrorAction SilentlyContinue
if ($null -ne $goCommand) {
    $goExecutable = $goCommand.Source
} else {
    $goExecutable = Join-Path $projectRoot ".tools\go\bin\go.exe"
}
if (-not (Test-Path -LiteralPath $goExecutable)) {
    throw "Go was not found. Install Go 1.26+ or place a portable toolchain in .tools\go."
}

New-Item -ItemType Directory -Path $artifactsRoot -Force | Out-Null
$env:GOOS = "windows"
$env:GOARCH = "amd64"
$env:CGO_ENABLED = "0"
& $goExecutable -C $daemonRoot build -trimpath -ldflags "-s -w -X main.version=$Version" -o $daemonExecutable .\cmd\bridge-daemon
if ($LASTEXITCODE -ne 0) { throw "Go daemon build failed." }

$selfContainedValue = $SelfContained.IsPresent.ToString().ToLowerInvariant()
$publishArguments = @(
    "publish",
    $desktopProject,
    "-c", "Release",
    "-r", "win-x64",
    "--self-contained", $selfContainedValue,
    "-p:Version=$Version",
    "-o", $publishDirectory,
    "--nologo"
)
if ($ExcludeSymbols) {
    $publishArguments += @("-p:DebugSymbols=false", "-p:DebugType=None")
}

dotnet @publishArguments
if ($LASTEXITCODE -ne 0) { throw "WPF Release publish failed." }
Copy-Item -LiteralPath $daemonExecutable -Destination (Join-Path $publishDirectory "bridge-daemon.exe") -Force
New-Item -ItemType Directory -Path (Join-Path $publishDirectory "licenses") -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $publishDirectory "docs") -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $projectRoot "THIRD_PARTY_NOTICES.md") -Destination $publishDirectory -Force
Copy-Item -LiteralPath (Join-Path $projectRoot "licenses\Apache-2.0.txt") -Destination (Join-Path $publishDirectory "licenses") -Force
Copy-Item -LiteralPath (Join-Path $projectRoot "licenses\gorilla-websocket-LICENSE.txt") -Destination (Join-Path $publishDirectory "licenses") -Force
Copy-Item -LiteralPath (Join-Path $projectRoot "docs\upstream-sources.md") -Destination (Join-Path $publishDirectory "docs") -Force

Write-Output "Build completed: $publishDirectory"
