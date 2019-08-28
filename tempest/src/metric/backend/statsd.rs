use crate::metric::backend::prelude::*;
use std::net::{ToSocketAddrs, UdpSocket};

#[allow(dead_code)]
pub(crate) struct Statsd {
    addr: String,
    prefix: Option<String>,
    socket: UdpSocket,
    target: MetricTarget,
    name_delimiter: Format,
    label_separator: Format,
    label_delimiter: Format,
}

impl Statsd {
    pub(crate) fn new(target: MetricTarget) -> BackendResult<Self> {
        let socket = match UdpSocket::bind("0.0.0.0:0") {
            Ok(_socket) => _socket,
            Err(err) => return Err(BackendError::from_io(err)),
        };

        let mut _prefix = None;
        let mut addr = "".to_string();
        match &target {
            MetricTarget::Statsd { host, prefix } => {
                addr = host.to_string();
                _prefix = prefix.clone();
            }
            _ => {}
        }

        // Try converting this address once
        // socket.send_to will automatically
        // do this each time, before sending
        &addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| BackendError::from_addr())?;

        Ok(Statsd {
            addr: addr,
            prefix: _prefix,
            socket: socket,
            target: target,
            name_delimiter: PERIOD,
            label_separator: EMPTY,
            label_delimiter: EMPTY,
        })
    }

    fn send(&self, metrics: &str) {
        match self.socket.send_to(&metrics.as_bytes(), &self.addr) {
            Ok(bytes) => {
                trace!("statd Ok({}) bytes written", bytes,);
            }
            Err(err) => {
                error!("statd error {:?}", err);
            }
        }
    }
}

impl Backend for Statsd {
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

        let mut out = String::new();
        let iter_mut = msg.metrics.bucket.map.iter_mut();
        let mut i = 0;
        let len = iter_mut.len();
        for (_key, metric) in iter_mut {
            let key = vec![name.clone(), metric.names.join(&self.name_delimiter)]
                .join(&self.name_delimiter);

            out.push_str(&key);
            out.push_str(COLON);
            if let FormatedMetric::Statsd(v) = &metric.to_value(MetricFormat::Statsd) {
                out.push_str(v);
            };
            out.push_str(PIPE);
            out.push_str(&metric.kind.as_statsd_str());
            i += 1;
            if i < len {
                out.push_str(LBR);
            }
        }
        warn!("statsd: {:?}", &out);
        self.send(&out);
    }
}

/// StatsdActor
///
pub(crate) struct StatsdActor {
    pub statsd: Statsd,
}
impl StatsdActor {}

impl Actor for StatsdActor {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {}
}

impl Handler<Msg> for StatsdActor {
    type Result = ();

    fn handle(&mut self, msg: Msg, _ctx: &mut Context<Self>) {
        self.statsd.write(msg);
    }
}
