(ns mapdag.test.runtime.default
  (:require [clojure.test :refer :all]
            [mapdag.test.core]
            [mapdag.runtime.default]))

(deftest compute--examples
  (mapdag.test.core/test-implementation--examples mapdag.runtime.default/compute))


(comment

  (run-all-tests #"mapdag\.test\..*")

  *e)

