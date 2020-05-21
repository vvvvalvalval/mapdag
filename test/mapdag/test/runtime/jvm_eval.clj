(ns mapdag.test.runtime.jvm-eval
  (:require [clojure.test :refer :all]
            [mapdag.test.core]
            [mapdag.runtime.jvm-eval]))


(deftest compute-default--examples
  (mapdag.test.core/test-implementation--examples
    (fn [graph inputs-map output-keys]
      (let [compute (mapdag.runtime.jvm-eval/compile-graph
                      {}
                      graph)]
        (compute inputs-map output-keys)))))


(deftest compute-with-known-input-and-ouput--examples
  (mapdag.test.core/test-implementation--examples
    (fn [graph inputs-map output-keys]
      (let [compute (mapdag.runtime.jvm-eval/compile-graph
                      {:mapdag.run/input-keys (keys inputs-map)
                       :mapdag.run/output-keys output-keys}
                      graph)]
        (compute inputs-map)))))



