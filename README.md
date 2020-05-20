# mapdag

A library for expressing computations as graphs of named steps, with dependencies between them.

More concretely, the computations are modeled as a Directed Acyclic Graphs (DAG) of named steps, represented as maps. Computation is performed by supplying inputs in a map, and returns the requested outputs in a map, hence the name `mapdag`.

The ideas behind this library are quite similar to [`plumatic/plumbing`'s Graph](https://github.com/plumatic/plumbing#graph-the-functional-swiss-army-knife), but this library is more focused, and makes some different design choices.

**TODO** usage
**TODO** Rationale
