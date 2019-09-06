# Traits

To create a `Source` library, you must implement these two traits.

The two traits below are packaged as part of the [`tempest-source`](https://docs.rs/crate/tempest-source/TEMPEST_SOURCE_VERSION) crate.

To build your own Source crate, add this to your `Cargo.toml` file:

```toml
tempest-source = "TEMPEST_SOURCE_VERSION"
```

## Source

The `Source` trait defines implementation details for brokering Topology messages. A bare-bones implementation only needs to implement `poll`.

For example, a library could read rows from a database, messages from a queue (Redis, Kafka, SQS, Kinesis, RabbitMQ, AMPQ, etc.), files, and construct messages which are returned when a Topology calls `poll`.

- [`Source trait` docs](https://docs.rs/tempest-source/TEMPEST_SOURCE_VERSION/tempest_source/prelude/trait.SourceBuilder.html)

## SourceBuilder

The `SourceBuilder` trait defines configuration details to instantiate structs that implement the `Source` trait.

- [`SourceBuilder trait` docs](https://docs.rs/tempest-source/TEMPEST_SOURCE_VERSION/tempest_source/prelude/trait.SourceBuilder.html)