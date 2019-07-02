use crate::metric::backend::prelude::*;

use std::time::{SystemTime, UNIX_EPOCH};

pub struct Prometheus {
    target: MetricTarget,
    name_delimiter: Format,
    label_separator: Format,
    label_delimiter: Format,
}

impl Prometheus {
    pub fn new(target: MetricTarget) -> Self {
        Prometheus {
            target: target,
            name_delimiter: UNDERSCORE,
            label_separator: EQUAL,
            label_delimiter: COMMA,
        }
    }

    fn push_gateway(&self, uri: &str, metrics: &str) {
        // https://github.com/prometheus/pushgateway/blob/master/README.md
        match minreq::post(uri).with_body(metrics).send() {
            Ok(http_result) => {
                trace!(
                    "pushgateway ok: {} {}",
                    http_result.status_code,
                    http_result.reason_phrase
                );
            }
            Err(err) => {
                error!("pushgateway error {:?}", err);
            }
        }
    }
}

impl Backend for Prometheus {
    fn write(&mut self, mut msg: Msg) {
        // Build the metric name
        // Add the config override
        let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => Some((duration.as_millis() as i64).to_string()),
            Err(err) => None,
        };
        let mut name = vec![];
        let mut push_uri = "";
        match &self.target {
            MetricTarget::Prometheus { uri, prefix } => {
                push_uri = uri;
                match prefix {
                    Some(p) => name.push(p.to_string()),
                    None => {}
                }
            }
            _ => {}
        }

        // Add the root prefix
        name.push(msg.root_prefix);

        // just clone the met
        if msg.metrics.names.len() > 0 {
            name.append(&mut msg.metrics.names.clone());
        }
        let name = name.join(&self.name_delimiter);

        // build out the labels
        let mut labels_map = HashMap::new();
        merge_labels(&mut labels_map, msg.root_labels);
        merge_labels(&mut labels_map, msg.metrics.labels);
        let has_lables = labels_map.len() > 0;
        let mut out = String::new();

        // https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
        for (key, metric) in msg.metrics.bucket.map.iter_mut() {
            // Construct the TYPE line
            let key = vec![name.clone(), metric.names.join(&self.name_delimiter)]
                .join(&self.name_delimiter);

            // annotate this metric
            out.push_str(POUND);
            out.push_str(SPACE);
            out.push_str("TYPE");
            out.push_str(SPACE);
            out.push_str(&key);
            out.push_str(SPACE);
            out.push_str(&metric.kind.as_prom_str());
            out.push_str(LBR);

            // We treat Prometheus as vec because
            // historgram/summary have multiple per metric

            if let FormatedMetric::Prometheus(values) = &metric.to_value(MetricFormat::Prometheus) {
                let root_key = key.clone();
                for item in values {
                    // item.0 = name,
                    // item.1 = label(key, value),
                    // item.2 = value
                    let value_key = if let Some(_name) = item.0 {
                        vec![root_key.clone(), _name.to_string()].join(&self.name_delimiter)
                    } else {
                        root_key.clone()
                    };
                    // appended value key
                    out.push_str(&value_key);

                    if has_lables || metric.labels.is_some() {
                        let mut labels = labels_map.clone();
                        merge_labels(&mut labels, metric.labels.clone());
                        out.push_str(LPAREN);
                        let mut iter = labels.into_iter();
                        let mut next = iter.next();
                        while let Some((k, v)) = next {
                            out.push_str(&k);
                            out.push_str(&self.label_separator);
                            out.push_str(QUOTE);
                            out.push_str(&v);
                            out.push_str(QUOTE);
                            next = iter.next();
                            if next.is_some() {
                                out.push_str(&self.label_delimiter);
                            }
                        }
                        if let Some(_label) = item.1 {
                            out.push_str(&self.label_delimiter);
                            out.push_str(&_label.0);
                            out.push_str(&self.label_separator);
                            out.push_str(QUOTE);
                            out.push_str(&_label.1);
                            out.push_str(QUOTE);
                        }
                        out.push_str(RPAREN);
                    }
                    out.push_str(SPACE);
                    out.push_str(&item.2);
                    // timestamp defaults to now are aren't supported
                    // via the pushgateway
                    out.push_str(LBR);
                }
            };
            self.push_gateway(&push_uri, &out);
            out.clear();
        }
    }
}

/// PrometheusActor
///
pub struct PrometheusActor {
    pub prometheus: Prometheus,
}
impl PrometheusActor {}

impl Actor for PrometheusActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Context<Self>) {}
}

impl Handler<Msg> for PrometheusActor {
    type Result = ();

    fn handle(&mut self, msg: Msg, ctx: &mut Context<Self>) {
        self.prometheus.write(msg);
    }
}
