use dipstick::*;

/// Track metrics
/// and flush them on schedule
///
/// We should define where these metrics should drain
/// and on what interval


// Metric bucket
pub struct Metric {
  namespace: String,
  bucket: AtomicBucket
}

impl Default for Metric {
  fn default() -> Metric {
    Metric {
      namespace: "default".into(),
      bucket: AtomicBucket::new()
    }
  }
}


impl Metric {

  pub fn new(namespace: String) -> Metric {
    Metric {
      namespace: namespace,
      bucket: AtomicBucket::new()
    }
  }

  pub fn count(&mut self, name: &str, count: usize) {
    self.bucket.counter(name).count(count);
  }

  pub fn gauge(&mut self, name: &str, value: isize) {
    self.bucket.gauge(name).value(value);
  }

  pub fn marker(&mut self, name: &str) {
      self.bucket.marker(name);
  }

  pub fn level(&mut self, name: &str, count: isize) {
      self.bucket.level(name).adjust(count);
  }

  pub fn timer(&mut self, name: &str) -> dipstick::Timer {
    self.bucket.timer(name)
  }

}