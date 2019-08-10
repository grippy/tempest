use crate::metric::backend::prelude::*;
use std::fs::OpenOptions;
use std::path::PathBuf;

pub struct File {
    prefix: Option<String>,
    target: MetricTarget,
    file: std::fs::File,
    name_delimiter: Format,
    label_separator: Format,
    label_delimiter: Format,
}

impl File {
    pub fn new(target: MetricTarget, target_name: String) -> BackendResult<Self> {
        let mut _path = "".to_string();
        let mut _clobber = false;
        let mut _prefix = None;

        match &target {
            MetricTarget::File {
                path,
                clobber,
                prefix,
            } => {
                _path = path.clone();
                if let Some(clobber) = clobber {
                    _clobber = clobber.clone();
                }
                _prefix = prefix.clone();
            }
            _ => {}
        };

        if _path.len() == 0 {
            return Err(BackendError::from_other(format!(
                "Backend target missing path: {:?}",
                target
            )));
        }
        // append root target name to path
        let mut path_buf = PathBuf::from(&_path);
        if target_name.len() > 0 {
            path_buf.push(&target_name);
        }

        let path = &path_buf.as_path();
        if let Some(file_name) = path.file_name() {
            // Skip creating directory, user should do this
            let _ = std::fs::create_dir_all(path.parent().unwrap());
        // if !path.parent().unwrap().exists() {
        //     warn!("Path doesn't exist: {:?}", &path.parent());
        // }
        } else {
            let _ = std::fs::create_dir_all(path);
            if !path.exists() {
                warn!("Path doesn't exist: {:?}", &path);
            }
        }

        if _clobber {
            let _ = std::fs::remove_file(&path_buf);
        }
        warn!("File name: {:?}", &path_buf);
        // open file
        let result = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path_buf);

        let file = match result {
            Ok(_file) => _file,
            Err(err) => return Err(BackendError::from_io(err)),
        };

        Ok(File {
            prefix: _prefix,
            target: target,
            file: file,
            name_delimiter: PERIOD,
            label_separator: COLON,
            label_delimiter: COMMA,
        })
    }
}

impl Backend for File {
    fn write(&mut self, mut msg: Msg) {
        // Build the metric name
        // Add the config override
        let mut name = vec![];
        // Add the prefix
        if let Some(p) = &self.prefix {
            name.push(p.to_string());
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

        // Each metric has labels... we need to clone
        // labels_map
        let has_lables = labels_map.len() > 0;

        let mut out = String::new();

        for (key, metric) in msg.metrics.bucket.map.iter_mut() {
            let key = vec![name.clone(), metric.names.join(&self.name_delimiter)]
                .join(&self.name_delimiter);

            out.push_str(&metric.kind.as_str());
            out.push_str(COLON);
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
            out.push_str(LBR);
            let result = &self.file.write_all(out.as_bytes());
            if result.is_err() {
                error!("Error writing to file: {:?}", result);
            }
            out.clear();
        }
        let result = &self.file.flush();
        if result.is_err() {
            error!("Error flushing file: {:?}", result);
        }
    }
}

/// FileActor
///
pub struct FileActor {
    pub file: File,
}
impl FileActor {}

impl Actor for FileActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Context<Self>) {}
}

impl Handler<Msg> for FileActor {
    type Result = ();

    fn handle(&mut self, msg: Msg, ctx: &mut Context<Self>) {
        self.file.write(msg);
    }
}
