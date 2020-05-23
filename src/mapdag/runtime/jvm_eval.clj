(ns mapdag.runtime.jvm-eval
  "AOT-compilation of graphs on the JVM, improving performance by leveraging JVM-specific code generation (via `clojure.core/eval`) and primitive values.

  Compared to the reference mapdag interpreter, addresses the following sources of execution overhead:

  1. Dynamic argument passing to the `:mapdag.step/compute-fn` (via `clojure.core/apply`). This implementation generates arity-aware code.
  2. Map-based navigation. This implementation generates code that navigates the graph through primitive array lookups.
  3. Megamorphic JVM call sites (when calling the `:mapdag.step/compute-fn`), impeding JIT optimization. This function eschews this by generating code which defines one function per step, and does static dispatch on the keys (using `clojure.core/case`).
  "
  (:require [mapdag.analysis]
            [cljfmt.core :as cljfmt]))

(defmacro explained-code
  "A noop macro for emitting self-commenting code.

  The first `expl-vec` argument (typically a vector) can be used for commenting, and won't be emitted in expansion.
  The unchanged second argument (or nil if not supplied) will be the result of macro-expansion."
  ([expl-vec] nil)
  ([expl-vec expr] expr))

;; IMPROVEMENT "won't throw" metadata on fn. (Val, 21 May 2020)
;; 4. catch clauses?

;; IDEA bit masks for representing ancestors set? ;; https://drumcoder.co.uk/blog/2010/jan/06/bitmasks-java/
;; Could be mighty useful when the input keys are known AOT.


(defn compute-fn-threw-ex
  [graph step-name deps-values causing-err]
  (ex-info
    (str "Error computing step " (pr-str step-name) ": " (pr-str :mapdag.step/compute-fn) " threw.")
    (merge
      {:mapdag.error/reason :mapdag.errors/compute-fn-threw
       :mapdag.trace/deps-values deps-values
       :mapdag.step/name step-name}
      (-> graph
        (get step-name)
        (dissoc :mapdag.step/compute-fn)
        (select-keys
          [:mapdag.step/deps])))
    causing-err))


(defn dep-cycle-ex
  [step-name]
  (ex-info
    (str "Found dependency cycle around : " (pr-str step-name))
    {:mapdag.error/reason :mapdag.errors/dependency-cycle
     :mapdag.step/name step-name}))


(defn missing-step-or-input-ex
  [step-name]
  (ex-info
    (str "Missing step or input: " (pr-str step-name))
    {:mapdag.error/reason :mapdag.errors/missing-step-or-input
     :mapdag.step/name step-name}))


(defn case-able-key?
  [k]
  (or
    (keyword? k)
    (string? k)
    (symbol? k)
    (number? k)
    (boolean? k)))


(defn compile-default
  [graph]
  (mapdag.analysis/validate-shape graph)
  (let [i->k (->> (into (set (keys graph))
                    (mapcat :mapdag.step/deps)
                    (vals graph))
               vec)
        k->i (into {}
               (map-indexed (fn [i k] [k i]))
               i->k)
        non-caseable-k->i
        (into {}
          (keep (fn [[k i]]
                  (when-not (case-able-key? k)
                    [k i])))
          k->i)
        N (count k->i)
        non-dag-keys (into []
                       (remove graph)
                       i->k)
        computed-arr-sym (vary-meta 'computed-arr
                           merge {:tag 'objects})]
    (letfn
      [(sym-suffix [k]
         (if (instance? clojure.lang.Named k)
           (str "--" (name k))
           ""))
       (sym-from-k [prefix k]
         (symbol
           (str prefix (k->i k)
             (sym-suffix k))))
       (step-name-sym [k] (sym-from-k "k-" k))
       (compute-fn-sym [k] (sym-from-k "compute-fn-" k))
       (resolve-fn-sym [k] (sym-from-k "resolve-" k))
       (local-value-sym [k] (sym-from-k "l-" k))]
      (let [factory-code
            `(explained-code
               ["This generated function gets injected some runtime data (notably the " :mapdag.step/compute-fn
                "functions), and returns the actual output of the compilation process: a function accepting"
                ~'[inputs-map output-keys] "arguments and returning a map."]
               (fn ~'factory [~'graph ~'k->i ~'i->k ~'non-caseable-k->i]
                 (let [~'MISSING (explained-code
                                   ["A flag object indicating a missing value."
                                    "Note that `nil` cannot be used for this purpose,"
                                    "as it is a valid value for Steps or inputs."]
                                   (Object.))
                       ~'RESOLVING (explained-code
                                     ["A flag object used for detecting dependency cycles."]
                                     (Object.))
                       ~'model-array
                       (explained-code
                         ["This array is a ready-to-be-cloned 'prototype' for the cache array"
                          ~'computed-arr
                          "which is why it's filled with"
                          ~'MISSING]
                         (object-array (repeat ~N ~'MISSING)))

                       ~'_EXPL-keys-locals
                       (explained-code
                         ["The following locals hold the values of the keys that name Steps or inputs"
                          ("usually keywords, e.g:" :myapp.stats/xs, :myapp.stats/N, :myapp.stats/mean "etc.")
                          "However, many other types are acceptable as keys, which is why we need these locals."])
                       ~@(->> i->k
                           (into []
                             (comp
                               (map-indexed
                                 (fn [i k]
                                   [(step-name-sym k)
                                    `(nth ~'i->k ~i)]))
                               cat)))

                       ~'_EXPL-keys-locals
                       (explained-code
                         ["The following locals hold the step-computing functions"
                          ("a.k.a" :mapdag.step/compute-fn)])
                       ~@(->> graph
                           keys
                           (mapcat
                             (fn [k]
                               [(compute-fn-sym k)
                                `(get-in ~'graph
                                   [~(step-name-sym k)
                                    :mapdag.step/compute-fn])])))]
                   (explained-code
                     ["The following functions perform the resolution of the computational Steps."
                      "There is one per Step, computing its value by resolving dependencies,"
                      "and using the cache array"
                      ~computed-arr-sym
                      "In particular, the call graph between these functions is static,"
                      "yielding monomorphic call sites amenable to JIT optimization."]
                     (letfn
                       [~@(->> non-dag-keys
                            (map
                              (fn [k]
                                (let [i (k->i k)
                                      k-sym (step-name-sym k)
                                      res-fn-sym (resolve-fn-sym k)]
                                  `(~res-fn-sym [~computed-arr-sym]
                                     (explained-code
                                       ["This function resolves input key"
                                        ~(if (case-able-key? k) k "<<NOT WRITEABLE>>")
                                        "by looking it up in the cache array"
                                        ~computed-arr-sym]
                                       (let [~'v (aget ~computed-arr-sym ~i)]
                                         (if (identical? ~'v ~'MISSING)
                                           (throw
                                             (missing-step-or-input-ex ~k-sym))
                                           ~'v))))))))

                        ~@(->> graph
                            (map
                              (fn [[k step]]
                                (let [i (k->i k)
                                      k-sym (step-name-sym k)
                                      res-fn-sym (resolve-fn-sym k)]
                                  `(~res-fn-sym [~computed-arr-sym]
                                     (explained-code
                                       ["This function resolves Step" ~(if (case-able-key? k) k "<<NOT WRITEABLE>>")]
                                       (let [~'v (aget ~computed-arr-sym ~i)]
                                         (if (identical? ~'v ~'MISSING)
                                           (explained-code ["Cache miss"]
                                             (do
                                               (explained-code ["In anticipation of dependency cycles:"]
                                                 (aset ~computed-arr-sym ~i ~'RESOLVING))
                                               (let [~@(->> step
                                                         :mapdag.step/deps
                                                         (mapcat
                                                           (fn [dk]
                                                             [(local-value-sym dk)
                                                              `(~(resolve-fn-sym dk) ~computed-arr-sym)])))
                                                     ~'v1
                                                     (explained-code
                                                       ["Having resolved the dependencies of this Step in the above locals,"
                                                        "We're now computing the Step value by calling the"
                                                        :mapdag.step/compute-fn]
                                                       ~(let [compute-expr
                                                              `(~(compute-fn-sym k)
                                                                 ~@(->> step
                                                                     :mapdag.step/deps
                                                                     (map local-value-sym)))]
                                                          (if (:mapdag.step/wont-throw step) ;; INTRO set this to true to declare that the compute-fn can be expected to not throw any error, which allows for optimizations in execution.
                                                            `(explained-code
                                                               ["No error catching is done here, as this Step is annotated with a truthy"
                                                                :mapdag.step/wont-throw]
                                                               ~compute-expr)
                                                            `(try
                                                               ~compute-expr
                                                               (catch Throwable ~'err
                                                                 (throw
                                                                   (compute-fn-threw-ex ~'graph ~k-sym
                                                                     ~(->> step
                                                                        :mapdag.step/deps
                                                                        (mapv local-value-sym))
                                                                     ~'err)))))))]
                                                 (explained-code ["Cache put"]
                                                   (aset ~computed-arr-sym ~i ~'v1))
                                                 ~'v1)))
                                           (if (identical? ~'v ~'RESOLVING)
                                             (explained-code ["Dependency cycle detected."]
                                               (throw
                                                 (dep-cycle-ex ~k-sym)))
                                             (explained-code ["Cache hit"]
                                               ~'v))))))))))]
                       (let [~'add-input
                             (explained-code
                               ["This function reads an inputs-map entry, adding its value to the"
                                ~'computed-arr
                                "cache array if required."]
                               (fn ~'add-input [~computed-arr-sym ~'k ~'v]
                                 (explained-code
                                   ["Static dispatch on input keys, hopefully makes things faster."]
                                   (case ~'k
                                     ~@(->> k->i
                                         (filter
                                           (fn [[k _i]]
                                             (case-able-key? k)))
                                         (mapcat
                                           (fn [[k i]]
                                             [k
                                              `(aset ~computed-arr-sym ~i ~'v)])))
                                     ~(if (empty? non-caseable-k->i)
                                        nil
                                        `(explained-code
                                           ["Handling input keys unsuitable for a (case ...) clause (very untypical)."]
                                           (when-some [~'i (get ~'non-caseable-k->i ~'k)]
                                             (aset ~computed-arr-sym ~'i ~'v))))))
                                 ~computed-arr-sym))

                             ~'k->resolve-fn
                             ~(into {}
                                (map (fn [[k _i]]
                                       [(step-name-sym k)
                                        (resolve-fn-sym k)]))
                                non-caseable-k->i)

                             ~'resolve-output-key
                             (explained-code
                               ["This function dynamically resolves a requested output key by dispatching to the"
                                ~'resolve-x--MY-KEY
                                "functions."]
                               (fn ~'resolve-output-key [~'inputs-map ~computed-arr-sym ~'output-k]
                                 (case ~'output-k
                                   ~@(->> i->k
                                       (filter case-able-key?)
                                       (mapcat
                                         (fn [k]
                                           (let [res-f-sym (resolve-fn-sym k)]
                                             [k
                                              `(~res-f-sym ~computed-arr-sym)]))))
                                   ~(let [find-in-inputs-expr
                                          `(explained-code
                                             ["The requested output-key is not declared in the graph,"
                                              "looking it up in the inputs map."]
                                             (if-some [[~'_k ~'v] (find ~'inputs-map ~'output-k)]
                                                ~'v
                                                (throw
                                                    (missing-step-or-input-ex ~'output-k))))]
                                      (if (empty? non-caseable-k->i)
                                        find-in-inputs-expr
                                        `(if-some [~'res-f (get ~'k->resolve-fn ~'output-k)]
                                           (explained-code
                                             ["The requested output-key is one of those that can't be matched in a `case` clause,"
                                              "so we fall back to a dynamic map-based lookup."]
                                             (~'res-f ~computed-arr-sym))
                                           ~find-in-inputs-expr))))))]
                         (explained-code
                           ["This function is the actual output of the graph compilation."
                            "It closes over the above-defined helper and constants."]
                           (fn ~'compiled-compute
                             [~'inputs-map ~'output-keys]
                             (let [;; IMPROVEMENT use topological sort to initialize an even shorter array (Val, 21 May 2020)
                                   ~computed-arr-sym (object-array ~N)
                                   ~'_ (explained-code
                                         ["Initializing a cache array filled with" ~'MISSING]
                                         (System/arraycopy
                                            ~'model-array 0
                                            ~computed-arr-sym 0 ~N))

                                   ~computed-arr-sym
                                   (explained-code
                                     ["Scanning the inputs map, filling the cache array"
                                      "with those values that will be used in downstream Step computations."
                                      "In particular, it's preferable for performance to have a small inputs-map."]
                                     ;; IMPROVEMENT make it possible to avoid this linear cost. (Val, 23 May 2020)
                                     ;; One issue to consider is that any graph key is a potential input.
                                     (reduce-kv ~'add-input ~computed-arr-sym ~'inputs-map))]
                               (persistent!
                                 (reduce
                                   (fn [~'t-ret ~'output-k]
                                     (assoc! ~'t-ret
                                       ~'output-k
                                       (~'resolve-output-key ~'inputs-map ~computed-arr-sym ~'output-k)))
                                   (transient {})
                                   ~'output-keys)))))))))))
            factory
            (binding
              [];[*warn-on-reflection* true]
              (-> factory-code
                ;; Uncomment to print the generated code.
                ;(doto (mapdag.dev/pprint-generated-code))
                eval))]
        (factory graph k->i i->k non-caseable-k->i)))))


(comment

  (def graph
    (-> mapdag.test.core/stats-dag
      (assoc-in [:N :mapdag.step/wont-throw] true)))

  (def compute (compile-default graph))

  *e

  (compute
    {:xs [1. 2. 3.]}
    [:mean :sum])

  (compute
    {:xs [1. 2. 3.]}
    [:mean :sum :xs])

  (compute
    {:xs [1. 2. 3.]}
    [])

  (compute
    {:xs [1. 2. 3.] :ys 42}
    [:mean :sum :xs])


  (compute
    {:xs [1. 2. 3.] :ys 42}
    [:mean :sum :xs])

  (compute
    {:ys 42}
    [:mean :sum :xs])

  *e)





(defn static-steps-plan
  [graph input-keys output-keys]
  (let [input-keys (set input-keys)]
    (letfn
      [(next-idx [step-name->ops]
         (count step-name->ops))
       (aux [step-name->ops step-name]
         (if (contains? step-name->ops step-name)
           step-name->ops
           (if (contains? input-keys step-name)
             (assoc step-name->ops
               step-name
               {:mapdag.step/name step-name
                :mapdag.plan/op ::read-input-key
                :mapdag.plan/step-idx (next-idx step-name->ops)})
             (if-some [[_ step] (find graph step-name)]
               (-> step-name->ops
                 (as-> step-name->ops
                   (reduce aux
                     step-name->ops
                     (:mapdag.step/deps step))
                   (assoc step-name->ops
                     step-name
                     (merge
                       {:mapdag.step/name step-name
                        :mapdag.plan/op ::compute-step
                        :mapdag.plan/step-idx (next-idx step-name->ops)}
                       (select-keys step
                         [:mapdag.step/deps
                          :mapdag.step/compute-fn])))))
               (throw
                 (ex-info "Should not happen after dependencies validation"
                   {}))))))]
      (let [step-name->ops (reduce aux {} output-keys)]
        (->> step-name->ops
          vals
          (sort-by :mapdag.plan/step-idx)
          vec)))))

(defn compile-with-known-input-and-outputs
  [graph input-keys output-keys]
  (mapdag.analysis/validate-graph graph input-keys output-keys)
  (let [output-keys (set output-keys)
        plan (-> (static-steps-plan graph input-keys output-keys)
               (->>
                 (mapv
                   (fn [{:as op
                         step-name :mapdag.step/name
                         step-idx :mapdag.plan/step-idx
                         op-type :mapdag.plan/op}]
                     (let [sym-suffix (if (instance? clojure.lang.Named step-name)
                                        (str "--" (name step-name))
                                        "")
                           gen-step-symbol (fn [prefix]
                                             (symbol (str prefix step-idx sym-suffix)))]
                       (-> op
                         (merge
                           {::value-sym (gen-step-symbol "value-sym-")
                            ::wrapped-fn-sym
                            (gen-step-symbol
                              (case op-type
                                ::read-input-key "read-input-"
                                ::compute-step "compute-step-"))}
                           (when (contains? output-keys step-name)
                             {::output-key-sym (gen-step-symbol "output-key-sym-")})
                           (when (= op-type ::read-input-key)
                             {::input-key-sym (gen-step-symbol "input-key-sym-")}))
                         (cond->
                           (= op-type ::compute-step)
                           (merge
                             {::compute-fn-sym (gen-step-symbol "compute-fn-sym-")})))))))
               (as-> plan
                 (let [step-name->value-sym
                       (into {}
                         (map (fn [op]
                                [(:mapdag.step/name op)
                                 (::value-sym op)]))
                         plan)]
                   (->> plan
                     (mapv
                       (fn [op]
                         (merge op
                           (when-let [deps (:mapdag.step/deps op)]
                             {::deps-values-syms
                              (mapv step-name->value-sym deps)}))))))))
        factory-code
        `(fn [~'plan]
           (let [~'NOT-FOUND (Object.)

                 ~@(->> plan
                     (filter ::input-key-sym)
                     (mapcat
                       (fn [op]
                         [(::input-key-sym op)
                          `(:mapdag.step/name
                             (nth ~'plan ~(:mapdag.plan/step-idx op)))])))

                 ~@(->> plan
                     (filter ::output-key-sym)
                     (mapcat
                       (fn [op]
                         [(::output-key-sym op)
                          `(:mapdag.step/name
                             (nth ~'plan ~(:mapdag.plan/step-idx op)))])))

                 ~@(->> plan
                     (filter ::compute-fn-sym)
                     (mapcat
                       (fn [op]
                         [(::compute-fn-sym op)
                          `(:mapdag.step/compute-fn
                             (nth ~'plan ~(:mapdag.plan/step-idx op)))])))

                 ~@(->> plan
                     (mapcat
                       (fn [op]
                         [(::wrapped-fn-sym op)
                          (let [argv
                                (case (:mapdag.plan/op op)
                                  ::read-input-key
                                  ['inputs-map]
                                  ::compute-step
                                  (::deps-values-syms op))
                                body
                                (case (:mapdag.plan/op op)
                                  ::read-input-key
                                  `(let [v# (get ~'inputs-map
                                              ~(::input-key-sym op)
                                              ~'NOT-FOUND)]
                                     (if (identical? v# ~'NOT-FOUND)
                                       (throw
                                         (ex-info
                                           (str "Missing step or input: " (pr-str ~(::input-key-sym op)))
                                           {:mapdag.error/reason :mapdag.errors/missing-step-or-input
                                            :mapdag.step/name ~(::input-key-sym op)}))
                                       v#))
                                  ::compute-step
                                  `(try
                                     (~(::compute-fn-sym op)
                                       ~@(::deps-values-syms op))
                                     (catch Throwable err#
                                       (let [step-name# (:mapdag.step/name
                                                          (nth ~'plan ~(:mapdag.plan/step-idx op)))]
                                         (throw
                                           (ex-info
                                             (str "Error computing step " (pr-str step-name#) ": " (pr-str :mapdag.step/compute-fn) " threw.")
                                             {:mapdag.error/reason :mapdag.errors/compute-fn-threw
                                              :mapdag.trace/deps-values [~@(::deps-values-syms op)]
                                              :mapdag.step/name step-name#
                                              ;; IMPROVEMENT not so lazily (Val, 20 May 2020)
                                              :mapdag.step/deps (:mapdag.step/deps
                                                                  (nth ~'plan ~(:mapdag.plan/step-idx op)))}
                                             err#))))))]
                            `(fn ~(::wrapped-fn-sym op)
                               ~argv
                               ~body))])))]
             (fn ~'compute-outputs [~'inputs-map]
               (let [~@(->> plan
                         (mapcat
                           (fn [op]
                             [(::value-sym op)
                              (let [args (case (:mapdag.plan/op op)
                                           ::read-input-key
                                           ['inputs-map]
                                           ::compute-step
                                           (::deps-values-syms op))]
                                `(~(::wrapped-fn-sym op) ~@args))])))]
                 ~(into {}
                    (comp
                      (filter ::output-key-sym)
                      (map
                        (fn [op]
                          [(::output-key-sym op)
                           (::value-sym op)])))
                    plan)))))
        factory
        (binding [*warn-on-reflection* true]
          (eval (-> factory-code #_(doto clojure.pprint/pprint))))]
    (factory plan)))




(defn compile-graph
  "Performs Ahead-Of-Time compilation of the supplied mapdag graph. Uses `clojure.core/eval`.

  When known in advance, the available input keys and requested output keys may be supplied,
  allowing for an even more optimized computation.

  Returns a function which accepts an inputs-map and (when not supplied AOT) output-keys,
  and returns a map."
  [{:as _opts
    output-keys :mapdag.run/output-keys
    input-keys :mapdag.run/input-keys
    :or {input-keys false
         output-keys false}}
   graph]
  (let [known-input-keys? (not (false? input-keys))
        known-output-keys? (not (false? output-keys))]
    (cond
      (and known-input-keys? known-output-keys?)
      (compile-with-known-input-and-outputs graph input-keys output-keys)

      ;; TODO known-outputs (Val, 21 May 2020)

      ;; IMPROVEMENT known-inputs (Val, 21 May 2020)

      :else
      (compile-default graph))))




(comment

  (def graph mapdag.test.core/stats-dag)


  (require 'mapdag.runtime.default)
  (require '[criterium.core :as bench])

  (def inputs-map
    {:xs [1. 2. 3.] :ys "something-else"})

  (def output-keys
    [:variance :mean :xs])

  (bench/quick-bench
    (mapdag.runtime.default/compute graph inputs-map output-keys))
  ;Evaluation count : 20220 in 6 samples of 3370 calls.
  ;             Execution time mean : 25.268332 µs
  ;    Execution time std-deviation : 6.794613 µs
  ;   Execution time lower quantile : 17.504584 µs ( 2.5%)
  ;   Execution time upper quantile : 34.092603 µs (97.5%)
  ;                   Overhead used : 2.834925 ns


  (let [compute (compile-graph
                  {}
                  graph)]
    (bench/bench
      (compute inputs-map output-keys))
    compute)
  ;Evaluation count : 30349020 in 60 samples of 505817 calls.
  ;             Execution time mean : 1.989625 µs
  ;    Execution time std-deviation : 115.053590 ns
  ;   Execution time lower quantile : 1.791156 µs ( 2.5%)
  ;   Execution time upper quantile : 2.250594 µs (97.5%)
  ;                   Overhead used : 2.834925 ns
  ;
  ;Found 4 outliers in 60 samples (6.6667 %)
  ;	low-severe	 1 (1.6667 %)
  ;	low-mild	 3 (5.0000 %)
  ; Variance from outliers : 43.4230 % Variance is moderately inflated by outliers                 Overhead used : 2.834925 ns


  (let [compute (compile-graph
                  {:mapdag.run/input-keys [:xs]
                   :mapdag.run/output-keys output-keys}
                  graph)]
    (bench/bench
      (compute inputs-map)))
  ;Evaluation count : 50960100 in 60 samples of 849335 calls.
  ;             Execution time mean : 1.238251 µs
  ;    Execution time std-deviation : 56.319528 ns
  ;   Execution time lower quantile : 1.144463 µs ( 2.5%)
  ;   Execution time upper quantile : 1.348403 µs (97.5%)
  ;                   Overhead used : 2.834925 ns
  ;
  ;Found 1 outliers in 60 samples (1.6667 %)
  ;	low-severe	 1 (1.6667 %)
  ; Variance from outliers : 31.9346 % Variance is moderately inflated by outliers                 Overhead used : 2.834925 ns



  (require '[clj-async-profiler.core :as prof])

  (def c-default
    (compile-graph
      {}
      graph))

  (prof/profile
    (dotimes [_ 10000]
      (c-default inputs-map output-keys)))

  (prof/serve-files 7000)

  *e)
