(ns mapdag.runtime.default
  "Reference implementation for executing mapdag Graphs - a portable interpreter."
  (:require [mapdag.step :as mdg]
            [mapdag.analysis]
            [mapdag.error]))


(defn compute
  "Executes a mapdag Graph on the supplied inputs to return the required outputs.

  Given:
  - `graph`: a mapdag Graph
  - `inputs map`: a map from input keys to input values
  - `output-keys`: a sequence of input or Step names (this sequence may be empty or nil)

  returns a map from the output keys to their computed values.

  Only the necessary intermediary computations will be performed.

  In the current implementation, dependency cycles will be tolerated if they can't be reached from the outputs or
  are broken by the inputs. However, this behaviour is an implementation detail that should not be relied upon."
  [graph inputs-map output-keys]
  (mapdag.analysis/validate-graph graph
    (keys inputs-map)
    output-keys)
  (letfn [(ensure-key [acc step-name]
            (if (contains? acc step-name)
              acc
              (if (contains? inputs-map step-name)
                (assoc acc
                  step-name
                  (get inputs-map step-name))
                (if-some [{:as step
                           compute-fn :mapdag.step/compute-fn
                           deps-names :mapdag.step/deps}
                          (get graph step-name)]
                  (let [acc1 (reduce ensure-key acc deps-names)
                        deps-values (mapv acc1 deps-names)
                        v (try
                            (apply compute-fn deps-values)
                            (catch #?(:clj Throwable :cljs :default) err
                              (throw
                                (ex-info
                                  (str "Error computing step " (pr-str step-name) ": " (pr-str :mapdag.step/compute-fn) " threw.")
                                  (merge
                                    {:mapdag.error/reason :mapdag.errors/compute-fn-threw
                                     :mapdag.trace/deps-values deps-values ;; INTRO a vector, the values taken by the dependencies of this Step when the error was thrown. (Val, 20 May 2020)
                                     :mapdag.step/name step-name}
                                    step)
                                  err))))]
                    (assoc acc1 step-name v))
                  (throw
                    (ex-info
                      (str "Missing step or input: " (pr-str step-name))
                      {:mapdag.error/reason :mapdag.errors/missing-step-or-input
                       :mapdag.step/name step-name}))))))]

    (let [computed (reduce ensure-key {} output-keys)]
      (into {}
        (map
          (fn [step-name]
            [step-name (get computed step-name)]))
        output-keys))))

(comment

  (def inputs-map
    {:xs [0. 1. 1.2 -1.2]})

  (def graph
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

  (compute
    graph
    inputs-map
    [:N :mean :variance])
  => {:N 4, :mean 0.25000000000000006, :variance 0.9075}

  (compute
    graph
    {}
    [:N :mean :variance])
  *e

  (compute
    graph
    {:xs ["I'm not a number!"]}
    [:N :mean :variance])
  *e

  (compute
    graph
    inputs-map
    [:N :DOES-NOT-EXIST])


  *e)
