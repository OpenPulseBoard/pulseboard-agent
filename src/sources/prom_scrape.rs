use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::config::{parse_duration_secs, PromScrapeConfig};
use crate::signal::{Labels, MetricKind, MetricSample, Signal};
use crate::web::Inspector;

pub struct PromScrapeSource {
    cfg: PromScrapeConfig,
}

impl PromScrapeSource {
    pub fn new(cfg: PromScrapeConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self, tx: mpsc::Sender<Signal>, inspector: Inspector) -> Result<()> {
        let interval_secs = parse_duration_secs(&self.cfg.interval).unwrap_or(30);
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        loop {
            ticker.tick().await;

            match scrape(&client, &self.cfg.url, &self.cfg.extra_labels).await {
                Ok(samples) => {
                    for s in samples {
                        inspector.record_source(&self.cfg.name, "metric");
                        if tx.send(Signal::Metric(s)).await.is_err() {
                            debug!("prom_scrape {}: pipeline channel closed", self.cfg.name);
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    warn!("prom_scrape {}: scrape error: {}", self.cfg.name, e);
                }
            }
        }
    }
}

async fn scrape(
    client: &reqwest::Client,
    url: &str,
    extra: &HashMap<String, String>,
) -> Result<Vec<MetricSample>> {
    let text = client
        .get(url)
        .header("Accept", "text/plain; version=0.0.4; charset=utf-8")
        .send()
        .await?
        .text()
        .await?;

    parse_prometheus_text(&text, extra)
}

fn parse_prometheus_text(text: &str, extra: &HashMap<String, String>) -> Result<Vec<MetricSample>> {
    let lines = prometheus_parse::Scrape::parse(text.lines().map(|s| Ok(s.to_owned())))
        .map_err(|e| anyhow::anyhow!("prometheus parse error: {e}"))?;

    let mut samples = vec![];
    for sample in lines.samples {
        let value = match sample.value {
            prometheus_parse::Value::Gauge(v) => v,
            prometheus_parse::Value::Counter(v) => v,
            prometheus_parse::Value::Untyped(v) => v,
            // Skip histograms/summaries for now — they arrive as multiple lines
            _ => continue,
        };

        let kind = match sample.value {
            prometheus_parse::Value::Counter(_) => MetricKind::Counter,
            _ => MetricKind::Gauge,
        };

        let mut labels: Labels = sample
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        labels.extend(extra.clone());

        samples.push(MetricSample {
            name: sample.metric.clone(),
            labels,
            value,
            timestamp_ms: sample.timestamp.timestamp_millis(),
            kind,
        });
    }
    Ok(samples)
}
