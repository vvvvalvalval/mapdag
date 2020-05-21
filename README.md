# mapdag

[![Clojars Project](https://img.shields.io/clojars/v/vvvvalvalval/mapdag.svg)](https://clojars.org/vvvvalvalval/mapdag)

A library for expressing computations as graphs of named steps, with dependencies between them.

More concretely, the computations are modeled as a Directed Acyclic Graphs (DAG) of named steps, represented as maps. Computation is performed by supplying inputs in a map, and returns the requested outputs in a map, hence the name `mapdag`.

The ideas behind this library are quite similar to [`plumatic/plumbing`'s Graph](https://github.com/plumatic/plumbing#graph-the-functional-swiss-army-knife), but this library is more focused, and makes some different design choices.

**Project status:** alpha quality.


## Installation

This library is available for download from [Clojars](https://clojars.org/vvvvalvalval/mapdag).

To add it as a dependency to your project, add this to your `deps.edn`:

```
  vvvvalvalval/mapdag {:mvn/version "0.2.0"}
```



## Usage

#### Defining a computation graph

Suppose your project frequently involves computing some statistics from a list of numbers.

You can define these computations using a **`mapdag` Graph**:

```clojure
(ns myapp.stats)


(def stats-dag
  {::N
   {:mapdag.step/deps [::raw-numbers]
    :mapdag.step/compute-fn
    (fn [raw-numbers]
      (count raw-numbers))}

   ::sum
   {:mapdag.step/deps [::raw-numbers]
    :mapdag.step/compute-fn
    (fn [raw-numbers]
      (apply + 0. raw-numbers))}

   ::mean
   {:mapdag.step/deps [::sum ::N]
    :mapdag.step/compute-fn
    (fn [sum N] (/ sum N))}

   ::sum-of-squares
   {:mapdag.step/deps [::raw-numbers]
    :mapdag.step/compute-fn
    (fn [raw-numbers]
      (apply + 0.
        (map #(* % %)
          raw-numbers)))}

   ::variance
   {:mapdag.step/deps [::sum-of-squares ::N ::mean]
    :mapdag.step/compute-fn
    (fn [s2 N m]
      (-
        (/ s2 N)
        (* m m)))}})
```

Note that this is just a plain Clojure data structure, which describes how various **Steps** are computed in terms of others.


#### Calling the graph

You can now run this graph by providing some inputs and requiring some outputs:


```clojure
(require '[mapdag.runtime.default :as mdg-run])

(mdg-run/compute
  stats-dag
  {::raw-numbers [-1. 0. 2. 3.]} ;; inputs
  [::N ::mean]) ;; requested outputs

;=> {:myapp.stats/N 4, :myapp.stats/mean 1.0}
```

The Steps you requested are returned in a map. Only the necessary steps will have been computed to achieve this result (in this example, these are `#{::N, ::sum, ::mean}`).


#### Concision helpers

Finally, you may also want to express the graph more concisely. This library provides some helpers for that:

```clojure
(require '[mapdag.step :as mdg])

(def stats-dag
  {::N (mdg/step [::raw-numbers]
         (fn [raw-numbers]
           (count raw-numbers)))
   ::sum (mdg/step [::raw-numbers]
           (fn [raw-numbers]
             (apply + 0. raw-numbers)))
   ::mean (mdg/step [::sum ::N]
            (fn [sum N] (/ sum N)))
   ::sum-of-squares (mdg/step [::raw-numbers]
                      (fn [raw-numbers]
                        (apply + 0.
                          (map #(* % %)
                            raw-numbers))))
   ::variance (mdg/step [::sum-of-squares ::N ::mean]
                (fn [s2 N m]
                  (-
                    (/ s2 N)
                    (* m m))))})
```


See also the [reference documentation](https://cljdoc.org/d/vvvvalvalval/mapdag) on cljdoc, and [`mapdag.test.core`](./test/mapdag/test/core.cljc) for more examples.


#### Ahead-Of-Time compilation

`mapdag.runtime.default` offers the most dynamic execution strategy, but it has quite some overhead, which may be a problem in high-performance scenarios.

If the graph structure is known in advance, Ahead-Of-Time compilation can make the downstream computations much more efficient.

[`mapdag.runtime.jvm-eval`](./src/mapdag/runtime/jvm_eval.clj) enables you to compile graphs to highly-efficient functions on the JVM, using `clojure.core/eval` and low-level interop to generate low-overhead, JIT-friendly JVM bytecode:


```clojure
(require '[mapdag.runtime.jvm-eval])

(def compute-stats
  (mapdag.runtime.jvm-eval/compile-graph {} stats-dag))

(compute-stats
  {::raw-numbers [-1. 0. 2. 3.]}
  [::N ::mean])
;=> #:myapp.stats{:N 4, :mean 1.0}
```


## Rationale

Many computations (those that don't involve branching flow control) can be naturally described as how each step can be computed in terms of previous steps.

In Clojure, the ready-made constructs we have for writing such programs are:

* Functions calling each other. Problem: this leads to some values being computed several times, harming performance.
* Successive computations in a `let`. Problem: this requires anticipating which steps are available as inputs, and which are available as outputs.

This library alleviates this issue, by enabling the programmer to expressive the computation declaratively as a graph, and later call that grpah with explicit inputs and outputs, thus separating the concerns of 'how to compute' and 'what to compute'.

In addition, because the graphs are generic data structures, it's straighforward for the programmer to program on top of them: merging them, querying them, instrumenting them, etc.

Finally, this opens the opportunity for implementing specialized execution engines for such graphs, allowing for improved performance, asynchronous / monadic behaviour, etc.


## License

Copyright © 2020 Valentin Waeselynck and contributors

Distributed under the MIT License.
