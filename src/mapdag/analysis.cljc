(ns mapdag.analysis
  "'Static' analysis of mapdag graphs - in particular validation."
  (:require [mapdag.step :as mdg]))


(defn- dependency-cycle-path
  [step-name steps-trace]
  (->> steps-trace
    (take-while #(not= step-name %))
    (cons step-name)
    (reverse)
    (into [step-name])))


(defn- dependency-cycle-str
  [step-name steps-trace]
  (->> (dependency-cycle-path step-name steps-trace)
    (map pr-str)
    (interpose " -> ")
    (apply str)))



(defn validate-shape
  "Performs basic 'data shape/types' validation on the given graph, throwing an error if an invariant violation is found, otherwise returning the graph unmodified.

  Some kinds of errors that won't be detected:
  - incorrect arity of :mapdag.step/compute-fn
  - dependencies-related invariant violations, such as dependency cycles or missing keys."
  [graph]
  (when-not (map? graph)
    (throw
      (ex-info
        "graph must be a map."
        {:mapdag.error/reason :mapdag.errors/invalid-graph
         :graph graph})))
  (run!
    (fn [[step-name step]]
      (when-not (map? step)
        (throw
          (ex-info
            (str "step " (pr-str step-name) " must be a map.")
            {:mapdag.error/reason :mapdag.errors/invalid-graph
             :mapdag.step/name step-name
             :mapdag/step step})))
      (let [dps (::mdg/deps step)]
        (when-not (sequential? dps)
          (throw
            (ex-info
              (str "step " (pr-str step-name)
                " must contain a sequence at key " (pr-str ::mdg/deps))
              {:mapdag.error/reason :mapdag.errors/invalid-graph
               :mapdag.step/name step-name
               :mapdag/step step}))))
      (let [cfn (::mdg/compute-fn step)]
        (when-not (fn? cfn)
          (throw
            (ex-info
              (str "step " (pr-str step-name)
                " must contain a function at key " (pr-str ::mdg/compute-fn))
              {:mapdag.error/reason :mapdag.errors/invalid-graph
               :mapdag.step/name step-name
               :mapdag/step step})))))
    graph)
  graph)


(defn validate-dependency-links
  "Performs validation on the dependency structure of the given graph, throwing an error if an invariant violation is found, otherwise returning the graph unmodified.

  The only key required in Step maps for this validation is :mapdag.step/deps, so this function is suitable for validating partially-constructed graphs.

  See the documentation of #'validate for the additional arguments."
  [graph input-keys-or-false output-keys-or-false]
  (let [start-keys (if (false? output-keys-or-false)
                     (keys graph)
                     output-keys-or-false)
        check-inputs? (not (false? input-keys-or-false))
        graph1 (-> {}
                 (into graph)
                 (into
                   (map (juxt
                          identity
                          (constantly {::mdg/deps []}))
                     (or input-keys-or-false []))))]
    (letfn [(check-key [acc step-name steps-trace]
              (if (contains? acc step-name)
                (let [step-v (get acc step-name)]
                  (if (list? step-v)
                    (throw
                      (ex-info
                        (str "Found dependency cycle: "
                          " " (dependency-cycle-str step-name steps-trace))
                        {:mapdag.error/reason :mapdag.errors/dependency-cycle ;; INTRO This error indicates a dependency cycle amongst DAG steps. (Val, 20 May 2020)
                         :mapdag.trace/deps-cycle (dependency-cycle-path step-name steps-trace)
                         :mapdag.step/name step-name}))
                    acc))
                (if-some [[_ step] (find graph1 step-name)]
                  (let [st1 (conj steps-trace step-name)]
                    (assoc
                      (reduce
                        (fn [acc step-name]
                          (check-key acc step-name st1))
                        (assoc acc
                          step-name
                          (conj steps-trace step-name))
                        (::mdg/deps step))
                      step-name :checked))
                  (if check-inputs?
                    (throw
                      (ex-info
                        (str "Missing step or input: " (pr-str step-name))
                        {:mapdag.error/reason :mapdag.errors/missing-step-or-input ;; INTRO This error indicates that some of the steps or inputs required for the computations are not present. Note that given the API semantics, it's impossible to distinguish between a missing input and a missing step. (Val, 20 May 2020)
                         :mapdag.step/name step-name}))
                    (assoc acc step-name :checked)))))]
      (reduce
        (fn [acc step-name]
          (check-key acc step-name (list)))
        {}
        start-keys)))
  graph)


(defn validate-graph
  "Performs validation on the given mapdag Graph, throwing an error if an invariant violation is found, otherwise returning the graph unmodified.

  The other arguments can be used to limit what the validation will cover:

  - if `input-keys-or-false` is a sequence of input names, will check if some inputs or steps are missing.
  Otherwise this argument should be set to the boolean `false` (not `nil`, which would be interpreted as an empty sequence.).
  - if `output-keys-or-false` is a sequence of step names, will only check for dependency cycles or missing steps on the sub-graph induced by the ancestors of these steps.
  Otherwise this argument should be set to the boolean `false` (not `nil`, which would be interpreted as an empty sequence.)."
  [graph input-keys-or-false output-keys-or-false]
  (validate-shape graph)
  (validate-dependency-links graph input-keys-or-false output-keys-or-false)
  graph)


(comment

  (validate-graph
    {:a (mdg/step [:b] (constantly nil))
     :b (mdg/step [:c] (constantly nil))
     :c (mdg/step [:d] (constantly nil))
     :d (mdg/step [:b] (constantly nil))}
    nil nil)
  ;Found dependency cycle:  :b -> :c -> :d -> :b

  (validate-graph
    {:a (mdg/step [:b] (constantly nil))
     :b (mdg/step [:c] (constantly nil))
     :c (mdg/step [:d] (constantly nil))}
    {}
    nil)
  ;Missing step or input: :d

  *e)


