# Tempest Source Traits

Contains everything required for implementing Tempest `Source` libraries.

The main components include:

- `Msg` & `MsgId` type definitions
- `Source` trait for working with sources.
- `SourceBuilder` trait for instantiating a struct that implements `Source`.
- `SourceResult`, `SourcePollResult`, and other common configuration enums

Take a look at these implementation examples:

- [Mock](https://github.com/grippy/tempest/tree/master/tempest-source-mock)
- [Redis Streams](https://github.com/grippy/tempest/tree/master/tempest-source-redis)
