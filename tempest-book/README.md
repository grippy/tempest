# Tempest Book

## Prerequisites

- `cargo install cargo-config`
- `cargo install mdbook`
- `graphviz` for generating graphs

## Rebuild book on changes

- `mdbook watch --open`

## Crate version placeholders

The book contains links to the current version of all `Tempest` crates on `docs.rs`.
Each link uses a placeholder to the version.

- `TEMPEST_VERSION`
- `TEMPEST_SOURCE_VERSION`

## Generate Book

At the root of the project, there's a file called `scripts/build-book.sh` which build the book and replaces all Crate version placeholders.

Run this from the workspace root:

- `make build-book`

## Graphs

Add new graphs to the `./dot` folder and don't forget to add the generation code to the `graphviz` Makefile target.

## Generate Graphs

To generate example `dot` files, run the Makefile target from this directory:

- `make graphviz`