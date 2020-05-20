(ns mapdag.step)


(defn step
  "Helper function for declaring a mapdag Step concisely."
  [deps-names compute-fn]
  {:mapdag.step/deps deps-names ;; INTRO a sequence of keys, the names of the other Steps or input values required to compute this step. Typically Clojure keywords (namespaced keywords are allowed and encouraged).
   :mapdag.step/compute-fn compute-fn}) ;; INTRO a function that accepts as many arguments as there are deps for this Step, and returns the computed value.


