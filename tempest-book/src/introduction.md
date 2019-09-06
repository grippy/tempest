# Introduction

Tempest is a message processing framework inspired by Apache Storm (hence the name).

This framework provides an abstraction on top of [Actix](https://github.com/actix/actix) (popular actor model) for defining and running `Topologies`.

A topology reads messages from a `Source` (i.e. spout) and links together `Tasks` (i.e. bolts) as a `Pipeline` in the form of a directed acyclic graph.
