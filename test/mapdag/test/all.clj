(ns mapdag.test.all
  (:require [clojure.test :refer :all]
            [mapdag.test.core]
            [mapdag.test.analysis]
            [mapdag.test.runtime.default]
            [mapdag.test.runtime.jvm-eval]))

(comment

  (clojure.test/run-all-tests #"mapdag\.test\..*")

  *e)
