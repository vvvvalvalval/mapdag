(ns mapdag.step)


(defn step
  "Helper function for declaring a mapdag Step concisely."
  [deps-names compute-fn]
  {:mapdag.step/deps deps-names ;; FIXME document key
   :mapdag.step/compute-fn compute-fn}) ;; FIXME document key


