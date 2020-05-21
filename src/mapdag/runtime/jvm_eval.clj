(ns mapdag.runtime.jvm-eval
  "AOT-compilation of graphs on the JVM, improving performance by leveraging JVM-specific code generation (via `clojure.core/eval`) and primitive values.

  Compared to the reference mapdag interpreter, addresses the following sources of execution overhead:

  1. Dynamic argument passing to the `:mapdag.step/compute-fn` (via `clojure.core/apply`). This implementation generates arity-aware code.
  2. Map-based navigation. This implementation generates code that navigates the graph through primitive array lookups.
  3. Megamorphic JVM call sites (when calling the `:mapdag.step/compute-fn`), impeding JIT optimization. This function eschews this by generating code which defines one function per step, and does static dispatch on the keys (using `clojure.core/case`).
  "
  (:require [mapdag.analysis]))

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
        N (count k->i)
        non-dag-keys (into []
                       (remove graph)
                       i->k)
        computed-arr-sym (vary-meta 'computed-arr
                           ;; FIXME
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
       (compute-fn-sym [k] (sym-from-k "compute-" k))
       (ensure-fn-sym [k] (sym-from-k "ensure-" k))
       (local-value-sym [k] (sym-from-k "l-" k))]
      (let [factory-code
            `(fn ~'factory [~'graph ~'k->i ~'i->k]
               (let [~'MISSING (Object.)
                     ~'EXPLORING (Object.)

                     ~@(->> i->k
                         (into []
                           (comp
                             (map-indexed
                               (fn [i k]
                                 [(step-name-sym k)
                                  `(nth ~'i->k ~i)]))
                             cat)))

                     ~@(->> graph
                         keys
                         (mapcat
                           (fn [k]
                             [(compute-fn-sym k)
                              `(get-in ~'graph
                                 [~(step-name-sym k)
                                  :mapdag.step/compute-fn])])))

                     ~'model-array (object-array (repeat ~N ~'MISSING))]
                 (letfn
                   [(~'add-input [~computed-arr-sym ~'k ~'v]
                      (case ~'k
                        ~@(->> k->i
                            (filter
                              (fn [[k _i]]
                                (case-able-key? k)))
                            (mapcat
                              (fn [[k i]]
                                [k
                                 `(aset ~computed-arr-sym ~i ~'v)])))
                        (when-some [~'i (get ~'k->i ~'k)] ;; IMPROVEMENT lookup in smaller map, removing caseable keys. (Val, 21 May 2020)
                          (aset ~computed-arr-sym ~'i ~'v)))
                      ~computed-arr-sym)

                    ~@(->> non-dag-keys
                        (map
                          (fn [k]
                            (let [i (k->i k)
                                  k-sym (step-name-sym k)
                                  ens-fn-sym (ensure-fn-sym k)]
                              `(~ens-fn-sym [~computed-arr-sym]
                                 (let [~'v (aget ~computed-arr-sym ~i)]
                                   (if (identical? ~'v ~'MISSING)
                                     (throw
                                       (missing-step-or-input-ex ~k-sym))
                                     ~'v)))))))

                    ~@(->> graph
                        (map
                          (fn [[k step]]
                            (let [i (k->i k)
                                  k-sym (step-name-sym k)
                                  ens-fn-sym (ensure-fn-sym k)]
                              `(~ens-fn-sym [~computed-arr-sym]
                                 (let [~'v (aget ~computed-arr-sym ~i)]
                                   (if (identical? ~'v ~'MISSING)
                                     (do
                                       (aset ~computed-arr-sym ~i ~'EXPLORING)
                                       (let [~@(->> step
                                                 :mapdag.step/deps
                                                 (mapcat
                                                   (fn [dk]
                                                     [(local-value-sym dk)
                                                      `(~(ensure-fn-sym dk) ~computed-arr-sym)])))
                                             ~'v1
                                             (try
                                               (~(compute-fn-sym k)
                                                 ~@(->> step
                                                     :mapdag.step/deps
                                                     (map local-value-sym)))
                                               (catch Throwable ~'err
                                                 (throw
                                                   (compute-fn-threw-ex ~'graph ~k-sym
                                                     ~(->> step
                                                        :mapdag.step/deps
                                                        (mapv local-value-sym))
                                                     ~'err))))]
                                         (aset ~computed-arr-sym ~i ~'v1)
                                         ~'v1))
                                     (if (identical? ~'v ~'EXPLORING)
                                       (throw
                                         (dep-cycle-ex ~k-sym))
                                       ~'v))))))))]
                   (let [~'k->ensure-fn
                         ~(into {}
                            (comp
                              (remove (fn [[k _i]] (case-able-key? k)))
                              (map (fn [[k _i]]
                                     [(step-name-sym k)
                                      (ensure-fn-sym k)])))
                            k->i)

                         ~'ensure-output-key
                         (fn ~'ensure-output-key [~'inputs-map ~computed-arr-sym ~'k]
                           (case ~'k
                             ~@(->> i->k
                                 (filter case-able-key?)
                                 (mapcat
                                   (fn [k]
                                     (let [ens-f-sym (ensure-fn-sym k)]
                                       [k
                                        `(~ens-f-sym ~computed-arr-sym)]))))
                             (if-some [~'ens-f (get ~'k->ensure-fn ~'k)]
                               (~'ens-f ~computed-arr-sym)
                               (if-some [[~'_k ~'v] (find ~'inputs-map ~'k)]
                                 ~'v
                                 (throw
                                   (missing-step-or-input-ex ~'k))))))]
                     (fn ~'compiled-compute
                       [~'inputs-map ~'output-keys]
                       (let [;; IMPROVEMENT use topological sort to initialize an even shorter array (Val, 21 May 2020)
                             ~computed-arr-sym (object-array ~N)
                             ~'_ (System/arraycopy
                                   ~'model-array 0
                                   ~computed-arr-sym 0 ~N)

                             ~computed-arr-sym (reduce-kv ~'add-input ~computed-arr-sym ~'inputs-map)]
                         (persistent!
                           (reduce
                             (fn [~'tret ~'ok]
                               (assoc! ~'tret
                                 ~'ok
                                 (~'ensure-output-key ~'inputs-map ~computed-arr-sym ~'ok)))
                             (transient {})
                             ~'output-keys))))))))
            factory
            (binding [] #_ [*warn-on-reflection* true]
              (-> factory-code
                ;; FIXME #_
                (doto clojure.pprint/pprint)
                eval))]
        (factory graph k->i i->k)))))

(comment ;; example of generated Clojure code

  (require '[mapdag.step :as mdg])

  (def stats-dag
    {:N (mdg/step [:xs] count)
     :sum (mdg/step [:xs]
            (fn [xs] (apply + 0. xs)))
     :mean (mdg/step [:sum :N]
             (fn [sum N]
               (/ sum N)))
     :squares (mdg/step [:xs]
                (fn [xs]
                  (mapv #(* % %) xs)))
     :sum-squares (mdg/step [:squares]
                    (fn [s2] (apply + 0. s2)))
     :variance (mdg/step [:mean :sum-squares :N]
                 (fn [mean sum-squares N]
                   (-
                     (/ sum-squares N)
                     (* mean mean))))
     :stddev (mdg/step [:variance]
               (fn [variance] (Math/sqrt variance)))})

  (def compute-stats
    (compile-default stats-dag))


  ;; The generated `factory-code`. I've just pretty-printed it, and done some re-formatting and commenting.
  (fn factory
    ;; This generated function gets injected some runtime data (notably the :mapdag.step/compute-fn functions)
    ;; and returns a `compiled-compute` function which does the actual computation of derived keys.
    [graph k->i i->k]
    (let [MISSING (Object.)                       ;; Flag object indicating a missing (not yet found or computed) step
          EXPLORING (Object.)                     ;; Flag object used for detecting circular dependencies

          ;; Retrieving the keys. These values are usually keywords (in our example: :N, :mean, :xs, etc.), but other behaviour is allowed.
          k-0--squares (nth i->k 0)
          k-1--mean (nth i->k 1)
          k-2--stddev (nth i->k 2)
          k-3--variance (nth i->k 3)
          k-4--xs (nth i->k 4)
          k-5--sum-squares (nth i->k 5)
          k-6--N (nth i->k 6)
          k-7--sum (nth i->k 7)

          ;; Retrieving the functions that compute individual steps.
          compute-6--N (get-in graph [k-6--N :mapdag.step/compute-fn])
          compute-7--sum (get-in graph [k-7--sum :mapdag.step/compute-fn])
          compute-1--mean (get-in graph [k-1--mean :mapdag.step/compute-fn])
          compute-0--squares (get-in graph [k-0--squares :mapdag.step/compute-fn])
          compute-5--sum-squares (get-in graph [k-5--sum-squares :mapdag.step/compute-fn])
          compute-3--variance (get-in graph [k-3--variance :mapdag.step/compute-fn])
          compute-2--stddev (get-in graph [k-2--stddev :mapdag.step/compute-fn])

          ;; A 'prototype' array for the cache array.
          model-array (object-array (repeat 8 MISSING))]
      (letfn
        [(add-input [computed-arr k v]
           ;; Reads an inputs-map entry, adding its value to the `computed-arr` cache array if required.
           (case k                                          ;; static dispatch, hopefully makes things faster.
             :squares
             (aset computed-arr 0 v)
             :mean
             (aset computed-arr 1 v)
             :stddev
             (aset computed-arr 2 v)
             :variance
             (aset computed-arr 3 v)
             :xs
             (aset computed-arr 4 v)
             :sum-squares
             (aset computed-arr 5 v)
             :N
             (aset computed-arr 6 v)
             :sum
             (aset computed-arr 7 v)
             ;; handling non case-able keys (very untypical) by falling back to a Map lookup.
             ;; Note to self: remove this the typical case where a match is impossible, to avoid paying for non-processed keys in inputs-map.
             (when-some [i (get k->i k)]
               (aset computed-arr i v)))
           computed-arr)

         ;; Reads an input value that is not a compute step, which will have been added to the cache array by `add-input`.
         (ensure-4--xs [computed-arr]
           (let [v (aget computed-arr 4)]
             (if (identical? v MISSING)
               (throw
                 (mapdag.runtime.jvm-eval/missing-step-or-input-ex k-4--xs))
               v)))

         ;; The following functions, one per graph Step, all compute the value 
         ;; for their respective Step, calling each other as per their dependencies,
         ;; and using the `computed-arr` array as a cache.
         ;; In particular, the call graph between these functions is static,
         ;; so hopefully this can help JIT optimization, enabling monomorphic dispatch,
         ;; inlining etc. TODO does this optimization really happen as hoped?
         (ensure-6--N [computed-arr]
           ;; I've commented on this one function, all the other work the same.
           (let [v (aget computed-arr 6)]
             (if (identical? v MISSING)
               ;; cache miss
               (do
                 (aset computed-arr 6 EXPLORING)            ;; anticipating deps cycles
                 (let [;; computing each :mapdag.step/deps (only one in this case):
                       l-4--xs (ensure-4--xs computed-arr)
                       ;; computing the Step value
                       v1
                       (try
                         (compute-6--N l-4--xs)             ;; calling the :mapdag.step/compute-fn
                         (catch Throwable err               ;; Error catching. TODO Does this hinder JIT optimization?
                           (throw
                             (mapdag.runtime.jvm-eval/compute-fn-threw-ex
                               graph
                               k-6--N
                               [l-4--xs]
                               err))))]
                   (aset computed-arr 6 v1)                 ;; cache put
                   v1))
               (if (identical? v EXPLORING)
                 ;; dependency cycle detected
                 (throw (mapdag.runtime.jvm-eval/dep-cycle-ex k-6--N))
                 ;; cache hit
                 v))))
         (ensure-7--sum [computed-arr]
           (let [v (aget computed-arr 7)]
             (if
               (identical? v MISSING)
               (do
                 (aset computed-arr 7 EXPLORING)
                 (let
                   [l-4--xs
                    (ensure-4--xs computed-arr)
                    v1
                    (try
                      (compute-7--sum l-4--xs)
                      (catch
                        Throwable
                        err
                        (throw
                          (mapdag.runtime.jvm-eval/compute-fn-threw-ex
                            graph
                            k-7--sum
                            [l-4--xs]
                            err))))]
                   (aset computed-arr 7 v1)
                   v1))
               (if
                 (identical? v EXPLORING)
                 (throw (mapdag.runtime.jvm-eval/dep-cycle-ex k-7--sum))
                 v))))
         (ensure-1--mean [computed-arr]
           (let [v (aget computed-arr 1)]
             (if
               (identical? v MISSING)
               (do
                 (aset computed-arr 1 EXPLORING)
                 (let
                   [l-7--sum
                    (ensure-7--sum computed-arr)
                    l-6--N
                    (ensure-6--N computed-arr)
                    v1
                    (try
                      (compute-1--mean l-7--sum l-6--N)
                      (catch
                        Throwable
                        err
                        (throw
                          (mapdag.runtime.jvm-eval/compute-fn-threw-ex
                            graph
                            k-1--mean
                            [l-7--sum l-6--N]
                            err))))]
                   (aset computed-arr 1 v1)
                   v1))
               (if
                 (identical? v EXPLORING)
                 (throw (mapdag.runtime.jvm-eval/dep-cycle-ex k-1--mean))
                 v))))
         (ensure-0--squares
           [computed-arr]
           (let
             [v (aget computed-arr 0)]
             (if
               (identical? v MISSING)
               (do
                 (aset computed-arr 0 EXPLORING)
                 (let
                   [l-4--xs
                    (ensure-4--xs computed-arr)
                    v1
                    (try
                      (compute-0--squares l-4--xs)
                      (catch
                        Throwable
                        err
                        (throw
                          (mapdag.runtime.jvm-eval/compute-fn-threw-ex
                            graph
                            k-0--squares
                            [l-4--xs]
                            err))))]
                   (aset computed-arr 0 v1)
                   v1))
               (if
                 (identical? v EXPLORING)
                 (throw (mapdag.runtime.jvm-eval/dep-cycle-ex k-0--squares))
                 v))))
         (ensure-5--sum-squares
           [computed-arr]
           (let
             [v (aget computed-arr 5)]
             (if
               (identical? v MISSING)
               (do
                 (aset computed-arr 5 EXPLORING)
                 (let
                   [l-0--squares
                    (ensure-0--squares computed-arr)
                    v1
                    (try
                      (compute-5--sum-squares l-0--squares)
                      (catch
                        Throwable
                        err
                        (throw
                          (mapdag.runtime.jvm-eval/compute-fn-threw-ex
                            graph
                            k-5--sum-squares
                            [l-0--squares]
                            err))))]
                   (aset computed-arr 5 v1)
                   v1))
               (if
                 (identical? v EXPLORING)
                 (throw (mapdag.runtime.jvm-eval/dep-cycle-ex k-5--sum-squares))
                 v))))
         (ensure-3--variance
           [computed-arr]
           (let
             [v (aget computed-arr 3)]
             (if
               (identical? v MISSING)
               (do
                 (aset computed-arr 3 EXPLORING)
                 (let
                   [l-1--mean
                    (ensure-1--mean computed-arr)
                    l-5--sum-squares
                    (ensure-5--sum-squares computed-arr)
                    l-6--N
                    (ensure-6--N computed-arr)
                    v1
                    (try
                      (compute-3--variance l-1--mean l-5--sum-squares l-6--N)
                      (catch
                        Throwable
                        err
                        (throw
                          (mapdag.runtime.jvm-eval/compute-fn-threw-ex
                            graph
                            k-3--variance
                            [l-1--mean l-5--sum-squares l-6--N]
                            err))))]
                   (aset computed-arr 3 v1)
                   v1))
               (if
                 (identical? v EXPLORING)
                 (throw (mapdag.runtime.jvm-eval/dep-cycle-ex k-3--variance))
                 v))))
         (ensure-2--stddev
           [computed-arr]
           (let
             [v (aget computed-arr 2)]
             (if
               (identical? v MISSING)
               (do
                 (aset computed-arr 2 EXPLORING)
                 (let
                   [l-3--variance
                    (ensure-3--variance computed-arr)
                    v1
                    (try
                      (compute-2--stddev l-3--variance)
                      (catch
                        Throwable
                        err
                        (throw
                          (mapdag.runtime.jvm-eval/compute-fn-threw-ex
                            graph
                            k-2--stddev
                            [l-3--variance]
                            err))))]
                   (aset computed-arr 2 v1)
                   v1))
               (if
                 (identical? v EXPLORING)
                 (throw (mapdag.runtime.jvm-eval/dep-cycle-ex k-2--stddev))
                 v))))]
        (let [k->ensure-fn {}
              ensure-output-key
              (fn ensure-output-key
                ;; This function computes the value for a requested output key.
                [inputs-map computed-arr k]
                (case k                                     ;; Static dispatch for performance, to prevent megamorphic call sites in particular.
                  :squares
                  (ensure-0--squares computed-arr)
                  :mean
                  (ensure-1--mean computed-arr)
                  :stddev
                  (ensure-2--stddev computed-arr)
                  :variance
                  (ensure-3--variance computed-arr)
                  :xs
                  (ensure-4--xs computed-arr)
                  :sum-squares
                  (ensure-5--sum-squares computed-arr)
                  :N
                  (ensure-6--N computed-arr)
                  :sum
                  (ensure-7--sum computed-arr)
                  ;; Handling output keys that are not case-able or not declared in the graph (rare)
                  (if-some [ens-f (get k->ensure-fn k)]
                    (ens-f computed-arr)
                    (if-some [[_k v] (find inputs-map k)]
                      v
                      (throw
                        (mapdag.runtime.jvm-eval/missing-step-or-input-ex k))))))]
          ;; The function that is the actual output of the graph compilation.
          ;; Closes over all the helper functions and constants above.
          (fn compiled-compute
            [inputs-map output-keys]
            (let [computed-arr (object-array 8)             ;; the cache array
                  ;; Initializing the cache with MISSING
                  _ (System/arraycopy model-array 0 computed-arr 0 8)
                  ;; Importing the inputs into the cache array
                  computed-arr (reduce-kv add-input computed-arr inputs-map)]
              ;; computing the output map
              (persistent!
                (reduce
                  (fn [tret ok]
                    (assoc!
                      tret
                      ok
                      (ensure-output-key inputs-map computed-arr ok)))
                  (transient {})
                  output-keys))))))))



  *e)


(comment

  (def graph mapdag.test.core/stats-dag)

  (def compute (compile-default graph))

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
