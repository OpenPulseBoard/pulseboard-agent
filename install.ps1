#Requires -Version 5.1
<#
.SYNOPSIS
    install.ps1 — PulseAgent one-line installer for Windows.

.DESCRIPTION
    Downloads the latest PulseAgent release, writes a starter configuration,
    and installs it as a Windows service. Mirrors the behaviour of install.sh
    on Linux/macOS.

.EXAMPLE
    # PowerShell, run as Administrator
    irm https://raw.githubusercontent.com/OpenPulseBoard/pulseboard-agent/main/install.ps1 | iex

.EXAMPLE
    # With pre-set values (skips the interactive prompts)
    $env:PULSEBOARD_URL = "https://workspace.pulseboard.cloud"
    $env:ENROLL_TOKEN   = "tok_..."
    irm https://raw.githubusercontent.com/OpenPulseBoard/pulseboard-agent/main/install.ps1 | iex
#>

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# ---------------------------------------------------------------------------
# Settings (overridable via environment variables)
# ---------------------------------------------------------------------------
$AgentVersion = if ($env:AGENT_VERSION) { $env:AGENT_VERSION } else { "latest" }
$InstallDir   = if ($env:INSTALL_DIR)   { $env:INSTALL_DIR }   else { Join-Path $env:ProgramFiles "PulseAgent" }
$ConfigDir    = if ($env:CONFIG_DIR)    { $env:CONFIG_DIR }    else { Join-Path $env:ProgramData "PulseAgent" }
$DataDir      = if ($env:DATA_DIR)      { $env:DATA_DIR }      else { Join-Path $ConfigDir "data" }
$GithubRepo   = "OpenPulseBoard/pulseboard-agent"
$ServiceName  = "PulseAgent"

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
function Write-Info     { param($m) Write-Host "[pulseagent] $m" -ForegroundColor Green }
function Write-Warn     { param($m) Write-Host "[pulseagent] $m" -ForegroundColor Yellow }
function Write-Headline { param($m) Write-Host ""; Write-Host $m -ForegroundColor White }
function Fail           { param($m) Write-Host "[pulseagent] $m" -ForegroundColor Red; exit 1 }

# ---------------------------------------------------------------------------
# Privilege / environment checks
# ---------------------------------------------------------------------------
function Test-Administrator {
    $identity  = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Get-Target {
    switch ($env:PROCESSOR_ARCHITECTURE) {
        "AMD64" { return "x86_64-pc-windows-msvc" }
        "ARM64" { return "aarch64-pc-windows-msvc" }
        default { Fail "Unsupported architecture: $($env:PROCESSOR_ARCHITECTURE)" }
    }
}

# ---------------------------------------------------------------------------
# Download + extract binary
# ---------------------------------------------------------------------------
function Install-Binary {
    param([string]$Target)

    if ($AgentVersion -eq "latest") {
        $url = "https://github.com/$GithubRepo/releases/latest/download/pulseagent-$Target.zip"
    } else {
        $url = "https://github.com/$GithubRepo/releases/download/$AgentVersion/pulseagent-$Target.zip"
    }

    Write-Info "Downloading pulseagent from $url ..."
    $tmp = Join-Path $env:TEMP ("pulseagent-" + [Guid]::NewGuid().ToString())
    New-Item -ItemType Directory -Path $tmp -Force | Out-Null
    try {
        $zip = Join-Path $tmp "pulseagent.zip"
        Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing
        Expand-Archive -Path $zip -DestinationPath $tmp -Force

        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
        $exe = Join-Path $InstallDir "pulseagent.exe"
        Copy-Item -Path (Join-Path $tmp "pulseagent.exe") -Destination $exe -Force
        Write-Info "Installed pulseagent to $exe"
    } finally {
        Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
    }
}

# ---------------------------------------------------------------------------
# Prompt helper (skips when the value is already set via environment variable)
# ---------------------------------------------------------------------------
function Read-Value {
    param([string]$Current, [string]$Message)
    if ($Current) { return $Current }
    return (Read-Host $Message)
}

# ---------------------------------------------------------------------------
# Write config
# ---------------------------------------------------------------------------
function Write-Config {
    param([string]$PulseboardUrl, [string]$Token)

    New-Item -ItemType Directory -Path $ConfigDir -Force | Out-Null
    New-Item -ItemType Directory -Path $DataDir   -Force | Out-Null

    # TOML basic strings treat backslashes as escapes; forward slashes are
    # accepted by the agent on Windows and avoid having to escape paths.
    $dataPath = $DataDir.Replace('\', '/')

    $config = @"
[agent]
data_dir       = "$dataPath"
log_level      = "info"
pulseboard_url = "$PulseboardUrl"
enroll_token   = "$Token"

[sources.host_metrics]
interval = "15s"

# Windows Event Log channels (parity with journald on Linux).
[[sources.windows_event_log]]
name     = "system"
channel  = "System"
interval = "15s"

[[sources.windows_event_log]]
name     = "application"
channel  = "Application"
interval = "15s"

[processors.batch]
max_size  = 1000
max_delay = "5s"

[processors.cardinality_guard]
max_series_per_metric = 2000

[targets.pulseboard]
"@

    $configPath = Join-Path $ConfigDir "agent.toml"
    # Write without a BOM so the TOML parser reads the first key correctly.
    [System.IO.File]::WriteAllText($configPath, $config, (New-Object System.Text.UTF8Encoding($false)))
    Write-Info "Config written to $configPath"
}

# ---------------------------------------------------------------------------
# Windows service
# ---------------------------------------------------------------------------
function Install-Service {
    $exe        = Join-Path $InstallDir "pulseagent.exe"
    $configPath = Join-Path $ConfigDir "agent.toml"
    # --service makes the binary hand off to the Service Control Manager so it
    # reports Running/Stopped correctly instead of timing out at start.
    $binPath    = "`"$exe`" --service --config `"$configPath`""

    $existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($existing) {
        if ($existing.Status -eq "Running") {
            Stop-Service -Name $ServiceName -Force
        }
        # sc.exe is the most reliable way to update an existing binPath.
        & sc.exe config $ServiceName binPath= $binPath start= auto | Out-Null
        Write-Info "Updated existing $ServiceName service"
    } else {
        New-Service -Name $ServiceName `
                    -BinaryPathName $binPath `
                    -DisplayName "PulseBoard telemetry agent" `
                    -Description "Ships host metrics, logs, and Windows Event Log entries to PulseBoard." `
                    -StartupType Automatic | Out-Null
        Write-Info "Created $ServiceName service"
    }

    # Restart on failure (10s delay), mirroring the systemd unit.
    & sc.exe failure $ServiceName reset= 86400 actions= restart/10000 | Out-Null

    Start-Service -Name $ServiceName
    Write-Info "$ServiceName service started"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
function Main {
    Write-Headline "PulseAgent installer"

    if (-not (Test-Administrator)) {
        Fail "Administrator privileges required. Re-run this in an elevated PowerShell window."
    }

    $target = Get-Target
    Write-Info "Detected target: $target"

    Install-Binary -Target $target

    Write-Headline "Configuration"
    $pbUrl = Read-Value $env:PULSEBOARD_URL "PulseBoard workspace URL (e.g. https://acme.pulseboard.cloud)"
    $token = Read-Value $env:ENROLL_TOKEN   "Enrolment token from the portal (Settings -> Agents -> Generate token)"

    if (-not $pbUrl) { Fail "PulseBoard workspace URL is required" }
    if (-not $token) { Fail "Enrolment token is required" }

    Write-Config -PulseboardUrl $pbUrl -Token $token

    Write-Headline "Service"
    Install-Service

    Write-Headline "Done!"
    Write-Info "PulseAgent installed successfully."
    Write-Info "View the signal inspector at: http://localhost:8000"
    Write-Host ""
}

Main
