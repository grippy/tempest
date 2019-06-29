use crate::metric::backend::prelude::*;

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

        warn!("push {:?}", uri);
        use http_req::request::Method;
        use http_req::request::Request;
        let mut writer = Vec::new();
        let result = Request::new(&uri.parse().unwrap())
                .method(Method::POST)
                .body(&metrics.as_bytes())
                .send(&mut writer);
        match result {
            Ok(resp) => {
                warn!("status: {:?} {:?}", resp.status_code(), resp.reason());
            },
            Err(err) => {
                error!("error {:?}", err);
            }
        }
    }
}

impl Backend for Prometheus {
    fn write(&mut self, mut msg: Msg) {
        // Build the metric name
        // Add the config override
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
            out.push_str(&key);

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
                out.push_str(RPAREN);
            }

            out.push_str(SPACE);
            out.push_str(&metric.to_value());
            // TODO: Add timestamp

            out.push_str(LBR);
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
