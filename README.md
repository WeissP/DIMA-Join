# DIMA Join

A distributed inequality stream join over sliding windows, built on Apache Storm.

## Build

Requires [Mill](https://mill-build.org) 1.0.4 and a JDK.

```bash
mill dima.compile
mill dima.test
mill dima.assembly
```

The fat JAR is written to `out/dima/assembly.dest/out.jar`.

## Run

```bash
storm jar out.jar de.rptu.cs.dbis.submitTopologySpo
```

## Layout

- `topology/` wires the Storm topologies (SPO, TAQ, NYC, BLOND, HouseBuyer)
- `component/` contains the spouts and bolts (Partition, Dispatcher, Storage, Joiner)
- `collection/` contains the skip list, bitset and sorted buffer used by Storage
- `domain/Config.scala` holds the topology parameters
