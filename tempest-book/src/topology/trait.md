# Trait

To construct a `Topology` you'll need to implement this trait.

- [`Topology trait` docs](https://docs.rs/tempest/TEMPEST_VERSION/tempest/topology/trait.Topology.html)

This trait is fairly simple: all you need to implement is a `builder` or `test_builder` method which return `TopologyBuilder` instances.

The `TopologyBuilder` stores a `Source`, `Pipeline`, and `TopologyOptions` (high-level configuration).
