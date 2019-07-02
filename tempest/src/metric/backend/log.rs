use crate::metric::backend::prelude::*;

/// Log Backend
///
pub struct Log {
    target: MetricTarget,
    name_delimiter: Format,
    label_separator: Format,
    label_delimiter: Format,
}

impl Log {
    pub fn new(target: MetricTarget) -> Self {
        Log {
            target: target,
            name_delimiter: PERIOD,
            label_separator: COLON,
            label_delimiter: COMMA,
        }
    }
}

impl Backend for Log {
    fn write(&mut self, mut msg: Msg) {
        // Build the metric name
        // Add the config override
        let mut name = vec![];
        let mut log_level = log::Level::Info;

        match &self.target {
            MetricTarget::Log { level, prefix } => {
                if let Some(lv) = level {
                    log_level = lv.to_level();
                }

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
        // apply root first
        merge_labels(&mut labels_map, msg.root_labels);
        // apply metrics labels (overriding root)
        merge_labels(&mut labels_map, msg.metrics.labels);

        // Each metric has labels... we need to clone labels_map
        let has_lables = labels_map.len() > 0;

        let mut out = String::new();
        let total = msg.metrics.bucket.map.len();

        for (key, metric) in msg.metrics.bucket.map.iter_mut() {
            let key = vec![name.clone(), metric.names.join(&self.name_delimiter)]
                .join(&self.name_delimiter);

            out.push_str(SPACE);
            out.push_str(&key);
            out.push_str(SPACE);
            if let FormatedMetric::Standard(v) = &metric.to_value(MetricFormat::Standard) {
                out.push_str(v);
            };
            if has_lables || metric.labels.is_some() {
                let mut labels = labels_map.clone();
                merge_labels(&mut labels, metric.labels.clone());
                // format as a json string
                out.push_str(SPACE);
                out.push_str(LPAREN);
                let mut iter = labels.into_iter();
                let mut next = iter.next();
                while let Some((k, v)) = next {
                    out.push_str(QUOTE);
                    out.push_str(&k);
                    out.push_str(QUOTE);
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
            log!(target: &metric.kind.as_str(), log_level, "{}", &out);
            out.clear();
        }
    }
}

/// LogActor
///
pub struct LogActor {
    pub log: Log,
}
impl LogActor {}

impl Actor for LogActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Context<Self>) {}
}

impl Handler<Msg> for LogActor {
    type Result = ();

    fn handle(&mut self, msg: Msg, ctx: &mut Context<Self>) {
        self.log.write(msg);
    }
}
