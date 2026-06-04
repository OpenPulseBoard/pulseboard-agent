# PulseAgent

> Zero-config telemetry collector for [PulseBoard](https://pulseboard.cloud). Get host metrics, logs, and traces into your dashboard in under 90 seconds.

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/pulseagent)](https://crates.io/crates/pulseagent)

---

## Quick start

```bash
# One-line install (Linux, requires systemd)
curl -fsSL https://raw.githubusercontent.com/OpenPulseBoard/pulseboard-agent/main/install.sh | bash
```

The installer will prompt for your workspace URL and an enrollment token (generate one at **Settings → Agents → Generate token** in the PulseBoard portal). Within 90 seconds you'll see CPU, memory, disk, network, and load metrics in your dashboard.

### Pre-set values (for automated provisioning)

```bash
PULSEBOARD_URL=https://acme.pulseboard.cloud \
ENROLL_TOKEN=tok_... \
bash install.sh
```

### Docker

```bash
docker run -d \
  -e PULSEBOARD_URL=https://acme.pulseboard.cloud \
  -e ENROLL_TOKEN=tok_... \
  --name pulseagent \
  ghcr.io/pulseboard/agent
```

### Kubernetes

Run one agent per node as a DaemonSet. It tails `/var/log/containers/*.log`
(CRI format) and collects node host metrics — no API-server permissions required.

**Plain manifest (one-line apply):**

```bash
kubectl create namespace pulseboard
kubectl -n pulseboard create secret generic pulseagent-enroll \
  --from-literal=pulseboard_url="https://acme.pulseboard.cloud" \
  --from-literal=enroll_token="tok_..."
kubectl apply -f https://raw.githubusercontent.com/OpenPulseBoard/pulseboard-agent/main/deploy/kubernetes/daemonset.yaml
```

**Helm:**

```bash
helm install pulseagent deploy/helm/pulseagent \
  --namespace pulseboard --create-namespace \
  --set pulseboard.url=https://acme.pulseboard.cloud \
  --set pulseboard.enrollToken=tok_... \
  --set extraLabels.cluster=prod-us-east
```

See [`deploy/`](deploy/) for the full chart and manifest, including how to bring
your own secret (`pulseboard.existingSecret`) and scope log collection to
specific namespaces.

---

## Configuration

After enrollment the agent stores a long-lived API key in `$DATA_DIR` (default `/var/lib/pulseagent`) and only needs a minimal config:

```toml
# /etc/pulseagent/agent.toml
[agent]
pulseboard_url = "https://acme.pulseboard.cloud"

[sources.host_metrics]
interval   = "15s"
collectors = ["cpu", "memory", "disk", "network", "load"]
```

See [`agent.example.toml`](agent.example.toml) for the full reference including log tailing, Prometheus scraping, processors, and relabelling.

---

## Features

### Sources

| Source | What it collects | Equivalent |
| --- | --- | --- |
| `host_metrics` | CPU (per-core + global), memory, disk, network, load average | node\_exporter |
| `file_logs` | Tail any file or glob; multiline support | Filebeat / promtail |
| `prom_scrape` | Scrape any Prometheus `/metrics` endpoint | Prometheus |
| `journald` | Follow the systemd journal (`journalctl --follow`), per-unit + priority filter | journald |
| `windows_event_log` | Poll Windows Event Log channels via `Get-WinEvent` | winlogbeat |
| `docker_logs` | Stream container stdout/stderr via the Docker CLI | Docker logging driver |
| `docker_stats` | Per-container CPU / memory / pids gauges | cAdvisor (subset) |
| `kubernetes_pods` | Tail `/var/log/containers/*.log` (CRI format) — no API access needed | promtail / Fluent Bit |
| `otlp` | Receive OTLP/HTTP metrics & logs on a local port | OTel Collector |

### Processors

| Processor | Purpose |
| --- | --- |
| `batch` | Accumulate signals and flush on size or time threshold |
| `relabel` | Prometheus-compatible relabel rules (keep / drop / replace / label\_map) |
| `transform` | Set / rename / remove labels and lift JSON fields, with `${line}` / `${label.X}` / `${json.X}` templates |
| `cardinality_guard` | Drop series that exceed a per-metric series budget before they hit the wire |
| `redact_pii` | Regex-based redaction of log lines and label values |

### Built-in live debugger

Access `http://localhost:8000` while the agent is running. The debugger is a tabbed UI:

- **Signals** — live stream of every metric and log flowing through the pipeline, filterable by kind and name, with per-source throughput counters.
- **Pipeline** — a live graph of the configured `sources → processors → target` flow, annotated with how many signals each stage passed and dropped.
- **Linter** — static + live checks on your config: missing target/sources, invalid relabel/redact regexes, metric sources without a cardinality guard, and high-cardinality label names. Live entries appear when an observed metric crosses the cardinality warning threshold.
- **Why dropped?** — paste a metric name or log substring and see exactly which stage dropped matching signals and the reason (e.g. *"relabel rule 2 (drop): source labels matched the regex"* or *"metric http_requests_total exceeded the configured series budget"*).
- **Health endpoint** — `GET /api/healthz` for liveness probes.

---

## Labels & multi-host

When you deploy more than one agent, every series needs labels that identify *where* it came from — otherwise `node_cpu_seconds_total` from host A and host B collapse into one line on the dashboard.

### What the agent stamps automatically

Every OTLP payload sent to PulseBoard carries these resource attributes, which the edge flattens into Prometheus-style labels on every series:

| Label | Value | Source |
| --- | --- | --- |
| `service.name` | `pulseagent` | constant |
| `agent.version` | crate version | constant |
| `host.name` | `hostname::get()` | OS hostname, lazy-init once |
| `instance` | same as `host.name` | mirrors Prom convention so dashboards work out of the box |
| `agent.id` | enrolled agent ID | stable per-agent identifier from enrollment |

This means `host_metrics` series (CPU, memory, disk, network, load) are automatically multi-host aware — the built-in Library recipes for **Linux Host** and **Docker** will work with zero extra config.

### What you must label yourself: `prom_scrape` targets

The agent does **not** rewrite labels on series it scrapes from third-party exporters. If you scrape `postgres_exporter`, `redis_exporter`, `nginx-prometheus-exporter`, your own app's `/metrics`, etc., you must attach `instance` (and usually `job`) yourself via `extra_labels`:

```toml
[[sources.prom_scrape.targets]]
url          = "http://db1.internal:9187/metrics"   # postgres_exporter
interval     = "15s"
extra_labels = { job = "postgres", instance = "db1.internal:9187" }

[[sources.prom_scrape.targets]]
url          = "http://db2.internal:9187/metrics"
interval     = "15s"
extra_labels = { job = "postgres", instance = "db2.internal:9187" }

[[sources.prom_scrape.targets]]
url          = "http://app1.internal:3000/metrics"  # your Node/Go/Python/JVM app
interval     = "15s"
extra_labels = { job = "checkout-api", instance = "app1.internal:3000" }
```

This is the contract every Library recipe (Postgres, Redis, NGINX, Node.js, Go, Python, Java JVM, Docker) assumes. Without distinct `instance` labels, all your replicas show up as a single series.

### Kubernetes / multi-cluster

The Kubernetes recipe slices by `cluster` and `namespace`. The `namespace` label comes from kube-state-metrics for free; the `cluster` label is only present if your federating Prometheus sets `external_labels: { cluster: <name> }`. Single-cluster users will see one entry in the dropdown — harmless.

---

## CLI reference

```
pulseagent [OPTIONS]

Options:
  -c, --config <FILE>      Config file path [default: /etc/pulseagent/agent.toml]
                           [env: PULSEAGENT_CONFIG]
      --log-level <LEVEL>  trace | debug | info | warn | error
                           [env: PULSEAGENT_LOG]
      --check              Validate config and exit
      --print-config       Print resolved config as JSON and exit
      --dry-run            Collect and process signals, print them, don't ship
      --ui-port <PORT>     Port for the built-in debug UI [default: 8000]
                           [env: PULSEAGENT_UI_PORT]
  -h, --help               Print help
  -V, --version            Print version
```

---

## Building from source

```bash
# Prerequisites: Rust 1.78+
cargo build --release
# Binary at: target/release/pulseagent
```

---

## How enrollment works

1. Generate a short-lived token in the PulseBoard portal (valid 30 minutes).
2. On first start the agent calls `POST /api/agent/v1/enroll` with the token and receives a permanent `agent_id` + API key.
3. The key is written to `$DATA_DIR/credentials.json`. Subsequent starts skip enrollment.
4. Every 60 seconds the agent calls `POST /api/agent/v1/checkin` so the portal can display version, last-seen, and config drift.

---

## Repo layout

```
pulseboard-agent/
├── Cargo.toml
├── agent.example.toml   # full annotated config reference
├── install.sh           # one-line installer
├── Dockerfile           # static musl build → distroless image
├── deploy/
│   ├── kubernetes/
│   │   └── daemonset.yaml   # zero-API DaemonSet (kubectl apply)
│   └── helm/pulseagent/     # Helm chart
└── src/
    ├── main.rs          # CLI entry point
    ├── config.rs        # TOML config model
    ├── signal.rs        # Signal enum (Metric | Log)
    ├── enrollment.rs    # enroll + checkin
    ├── lint.rs          # config linter (powers the debugger's Linter tab)
    ├── pipeline.rs      # source → processor → target orchestration
    ├── sources/
    │   ├── host_metrics.rs
    │   ├── file_logs.rs
    │   ├── prom_scrape.rs
    │   ├── journald.rs
    │   ├── windows_event_log.rs
    │   ├── docker.rs            # docker_logs + docker_stats
    │   ├── kubernetes_pods.rs
    │   └── otlp_receiver.rs
    ├── processors/
    │   ├── batch.rs
    │   ├── cardinality_guard.rs
    │   ├── relabel.rs
    │   ├── transform.rs
    │   └── redact_pii.rs
    ├── targets/
    │   └── pulseboard.rs   # OTLP JSON + Loki push
    └── web/
        └── mod.rs          # live debugger UI
```

---

## License

MIT — see [LICENSE](LICENSE). PulseBoard itself is AGPL-3.0; the agent is MIT intentionally so it can be embedded anywhere.
