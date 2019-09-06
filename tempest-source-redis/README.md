# Tempest Source for Redis

This library polls for messages from a Redis Streams source.

## Install

`Cargo.toml`

```
[dependencies]
tempest-source-redis = "0.1.0"
```

## Usage

```rust
use tempest_source_redis::prelude::*;

// create a type alias to the RedisStreamSourceBuilder
type Source = RedisStreamSourceBuilder<'a>;

// configure a topology with this source
struct MyTopology {}
impl Topology<Source> for MyTopology {
    // implementation code here
}
```

## Topology.toml Config

You can configure `RedisStreamSource` at runtime by adding this to your Topology configuration file:

```toml
[source.config]

# redis connection uri
uri = "redis://127.0.0.1:6379/0"

# redis stream key
key = "some-key"

# redis stream group to use
group = "abc234"

# number of stream messages to read per xreadgroup
read_msg_count = 1000

# time in milliseconds for how often
# to poll for new stream messages
poll_interval = 1

# max poll backoff in milliseconds
max_backoff = 30000

# configure blocking reads
blocking_read = true

# msgid to begin reading from
group_starting_id = "0000"

# how often should the source check for new
# message to batch ack
ack_interval = 100

# ack strategy
ack_policy = { type = "Batch", value = 1000}
ack_policy = { type = "Individual" }

# monitor interval is used to handling messages
# from xpending
monitor_interval = 10000

# actions to take on pending message
pending_handlers = [
  { action = {type = "Delete"}, min_idle_time = 100000, times_delivered = 5},
  { action = {type = "Ack"}, min_idle_time = 10000, times_delivered = 10},
  { action = {type = "Claim"}, min_idle_time = 200000, times_delivered = 1},
]
```