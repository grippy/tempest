# Sources

A source provides an implementation for reading messages from a broker (or other means).

As part of this implementation, it should provide configuration options using the builder pattern.

# Configuration

- A source may define builder options
- A source may define how to parse `[source.config]` sections from `Topology.toml` configuration.

[See Topology.toml for more details](../runtime/topology-toml.md)
