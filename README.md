# PulseAgent

> Zero-config telemetry collector for [PulseBoard](https://pulseboard.cloud). Get host metrics, logs, and traces into your dashboard in under 90 seconds.

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/pulseagent)](https://crates.io/crates/pulseagent)

---

## Quick start

```bash
# One-line install (Linux, requires systemd)
curl -fsSL https://raw.githubusercontent.com/pulseboard/pulseboard-agent/main/install.sh | bash
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

### Sources (Wave 1)

| Source | What it collects | Equivalent |
| --- | --- | --- |
| `host_metrics` | CPU (per-core + global), memory, disk, network, load average | node\_exporter |
| `file_logs` | Tail any file or glob; multiline support | Filebeat / promtail |
| `prom_scrape` | Scrape any Prometheus `/metrics` endpoint | Prometheus |

### Processors

| Processor | Purpose |
| --- | --- |
| `batch` | Accumulate signals and flush on size or time threshold |
| `relabel` | Prometheus-compatible relabel rules (keep / drop / replace / label\_map) |
| `cardinality_guard` | Drop series that exceed a per-metric series budget before they hit the wire |
| `redact_pii` | Regex-based redaction of log lines and label values |

### Built-in live debugger

Access `http://localhost:8000` while the agent is running:

- **Signal Inspector** — live stream of every metric and log flowing through the pipeline, with pre- and post-processor payloads side-by-side.
- **Source stats** — throughput counters per source.
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
└── src/
    ├── main.rs          # CLI entry point
    ├── config.rs        # TOML config model
    ├── signal.rs        # Signal enum (Metric | Log)
    ├── enrollment.rs    # enroll + checkin
    ├── pipeline.rs      # source → processor → target orchestration
    ├── sources/
    │   ├── host_metrics.rs
    │   ├── file_logs.rs
    │   └── prom_scrape.rs
    ├── processors/
    │   ├── batch.rs
    │   ├── cardinality_guard.rs
    │   ├── relabel.rs
    │   └── redact_pii.rs
    ├── targets/
    │   └── pulseboard.rs   # OTLP JSON + Loki push
    └── web/
        └── mod.rs          # live debugger UI
```

---

## License

MIT — see [LICENSE](LICENSE). PulseBoard itself is AGPL-3.0; the agent is MIT intentionally so it can be embedded anywhere.
