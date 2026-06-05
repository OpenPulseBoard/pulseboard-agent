#!/usr/bin/env bash
# install.sh — PulseAgent one-line installer
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/OpenPulseBoard/pulseboard-agent/main/install.sh | bash
#
# Or with pre-set values:
#   PULSEBOARD_URL=https://workspace.pulseboard.cloud \
#   ENROLL_TOKEN=tok_... \
#   bash install.sh

set -euo pipefail

AGENT_VERSION="${AGENT_VERSION:-latest}"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
CONFIG_DIR="${CONFIG_DIR:-/etc/pulseagent}"
DATA_DIR="${DATA_DIR:-/var/lib/pulseagent}"
SYSTEMD_DIR="/etc/systemd/system"
GITHUB_REPO="OpenPulseBoard/pulseboard-agent"

BOLD="\033[1m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
RESET="\033[0m"

info()    { echo -e "${GREEN}[pulseagent]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[pulseagent]${RESET} $*"; }
error()   { echo -e "${RED}[pulseagent]${RESET} $*" >&2; exit 1; }
headline(){ echo -e "\n${BOLD}$*${RESET}"; }

# ---------------------------------------------------------------------------
# Detect OS / arch
# ---------------------------------------------------------------------------
detect_target() {
    local os arch
    os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    arch="$(uname -m)"
    case "$arch" in
        x86_64)  arch="x86_64" ;;
        aarch64|arm64) arch="aarch64" ;;
        *) error "Unsupported architecture: $arch" ;;
    esac
    case "$os" in
        linux)  echo "${arch}-unknown-linux-musl" ;;
        darwin) echo "${arch}-apple-darwin" ;;
        *) error "Unsupported OS: $os" ;;
    esac
}

# ---------------------------------------------------------------------------
# Download binary
# ---------------------------------------------------------------------------
download_binary() {
    local target="$1"
    local url

    if [[ "$AGENT_VERSION" == "latest" ]]; then
        url="https://github.com/${GITHUB_REPO}/releases/latest/download/pulseagent-${target}.tar.gz"
    else
        url="https://github.com/${GITHUB_REPO}/releases/download/${AGENT_VERSION}/pulseagent-${target}.tar.gz"
    fi

    info "Downloading pulseagent from ${url} …"
    local tmp_dir
    tmp_dir="$(mktemp -d)"
    trap "rm -rf ${tmp_dir}" EXIT

    if command -v curl &>/dev/null; then
        curl -fsSL "$url" | tar -xz -C "$tmp_dir"
    elif command -v wget &>/dev/null; then
        wget -qO- "$url" | tar -xz -C "$tmp_dir"
    else
        error "curl or wget is required"
    fi

    install -m 755 "${tmp_dir}/pulseagent" "${INSTALL_DIR}/pulseagent"
    info "Installed pulseagent to ${INSTALL_DIR}/pulseagent"
}

# ---------------------------------------------------------------------------
# Prompt helpers
# ---------------------------------------------------------------------------
prompt() {
    local var="$1" msg="$2" default="${3:-}"
    if [[ -n "${!var:-}" ]]; then
        return  # already set via env var
    fi
    if [[ -n "$default" ]]; then
        read -rp "$(echo -e "${BOLD}${msg}${RESET} [${default}]: ")" input
        printf -v "$var" '%s' "${input:-$default}"
    else
        read -rp "$(echo -e "${BOLD}${msg}${RESET}: ")" input
        printf -v "$var" '%s' "$input"
    fi
}

# ---------------------------------------------------------------------------
# Write config
# ---------------------------------------------------------------------------
write_config() {
    local pb_url="$1" token="$2"
    mkdir -p "$CONFIG_DIR"
    cat > "${CONFIG_DIR}/agent.toml" <<EOF
[agent]
data_dir       = "${DATA_DIR}"
log_level      = "info"
pulseboard_url = "${pb_url}"
enroll_token   = "${token}"

[sources.host_metrics]
interval = "15s"

[processors.batch]
max_size  = 1000
max_delay = "5s"

[processors.cardinality_guard]
max_series_per_metric = 2000

[targets.pulseboard]
EOF
    chmod 600 "${CONFIG_DIR}/agent.toml"
    info "Config written to ${CONFIG_DIR}/agent.toml"
}

# ---------------------------------------------------------------------------
# Systemd unit
# ---------------------------------------------------------------------------
write_systemd_unit() {
    if [[ ! -d "$SYSTEMD_DIR" ]]; then
        warn "systemd not found — skipping service installation"
        return
    fi

    cat > "${SYSTEMD_DIR}/pulseagent.service" <<EOF
[Unit]
Description=PulseBoard telemetry agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${INSTALL_DIR}/pulseagent --config ${CONFIG_DIR}/agent.toml
Restart=on-failure
RestartSec=10
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ReadWritePaths=${DATA_DIR}
WorkingDirectory=${DATA_DIR}
User=pulseagent
Group=pulseagent

[Install]
WantedBy=multi-user.target
EOF

    # Create dedicated system user if it doesn't exist
    if ! id -u pulseagent &>/dev/null; then
        useradd --system --no-create-home --shell /sbin/nologin pulseagent 2>/dev/null || true
    fi

    mkdir -p "$DATA_DIR"
    chown pulseagent:pulseagent "$DATA_DIR" 2>/dev/null || true

    # Hand config ownership to the service user so it can read the file
    # (chmod 600 was set at write time; root:pulseagent + 640 keeps it private)
    chown root:pulseagent "${CONFIG_DIR}/agent.toml" 2>/dev/null || true
    chmod 640 "${CONFIG_DIR}/agent.toml" 2>/dev/null || true
    chown root:pulseagent "${CONFIG_DIR}" 2>/dev/null || true
    chmod 750 "${CONFIG_DIR}" 2>/dev/null || true

    systemctl daemon-reload
    systemctl enable  pulseagent
    systemctl restart pulseagent
    info "pulseagent service enabled and started"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    headline "PulseAgent installer"

    # Check for root when installing system-wide
    if [[ "$(id -u)" -ne 0 ]] && [[ "$INSTALL_DIR" == /usr/* ]]; then
        error "Root privileges required for system-wide install. Try: sudo bash install.sh"
    fi

    local target
    target="$(detect_target)"
    info "Detected target: ${target}"

    download_binary "$target"

    headline "Configuration"
    local PULSEBOARD_URL="${PULSEBOARD_URL:-}"
    local ENROLL_TOKEN="${ENROLL_TOKEN:-}"
    prompt PULSEBOARD_URL "PulseBoard workspace URL (e.g. https://acme.pulseboard.cloud)"
    prompt ENROLL_TOKEN   "Enrolment token from the portal (Settings → Agents → Generate token)"

    write_config "$PULSEBOARD_URL" "$ENROLL_TOKEN"

    headline "Service"
    if [[ "$(uname -s)" == "Linux" ]]; then
        write_systemd_unit
    else
        warn "Non-Linux OS — systemd service not installed."
        info  "Start manually with: pulseagent --config ${CONFIG_DIR}/agent.toml"
    fi

    headline "Done!"
    info "PulseAgent installed successfully."
    info "View the signal inspector at: http://localhost:8000"
    echo
}

main "$@"
