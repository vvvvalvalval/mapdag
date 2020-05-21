(ns mapdag.test.core
  (:require [clojure.test :refer :all]
            [mapdag.step :as mdg]
            [mapdag.test.analysis]
            [mapdag.test.error]))

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



(defn test-implementation--examples
  [call-dag]
  (testing "(Examples on stats-dag)"
    (testing "Nominal cases: #'compute "
      (testing "returns exactly the required output keys, with correct values:"
        (is
          (=
            {:N 4, :mean 0.25000000000000006, :variance 0.9075}
            (call-dag
              stats-dag
              {:xs [0. 1. 1.2 -1.2]}
              [:N :mean :variance]))))
      (testing "can return inputs as well"
        (is
          (=
            {:xs [0. 1. 1.2 -1.2]}
            (call-dag
              stats-dag
              {:xs [0. 1. 1.2 -1.2]}
              [:xs]))))
      (testing "when the output keys are empty, returns an empty result:"
        (is
          (=
            {:N 4, :mean 0.25000000000000006, :variance 0.9075}
            (call-dag
              stats-dag
              {:xs [0. 1. 1.2 -1.2]}
              [:N :mean :variance]))))
      (testing "intermediary values may be supplied as input to avoid re-computation:"
        (is
          (=
            {:mean 2.0}
            (call-dag
              stats-dag
              {:N 2 :sum 4.}
              [:mean])))))
    (testing "Runtime errors: #'compute"
      (testing "when a Step fn throws, will throw wrapped errors with trace information:"
        (is
          (=
            [true
             {:mapdag.error/reason :mapdag.errors/compute-fn-threw,
              :mapdag.trace/deps-values [1.0 0],
              :mapdag.step/name :mean,
              :mapdag.step/deps [:sum :N]}]
            (-> (call-dag
                  stats-dag
                  {:N 0 :sum 1. :sum-squares 1.}
                  [:variance])
              (try
                nil
                (catch Throwable err
                  err))
              (as-> err
                (do
                  [(-> err ex-cause some?)
                   #_(-> err ex-message)
                   (mapdag.test.error/error-api-data err)]))))))
      (testing "will report missing steps or inputs:"
        (is
          (=
            {:mapdag.error/reason :mapdag.errors/missing-step-or-input,
             :mapdag.step/name :xs}
            (mapdag.test.error/catch-error-api-data
              (call-dag
                stats-dag
                {}
                [:variance]))))
        (is
          (=
            {:mapdag.error/reason :mapdag.errors/missing-step-or-input,
             :mapdag.step/name :THIS-OUTPUT-KEY-DOES-NOT-EXIST}
            (mapdag.test.error/catch-error-api-data
              (call-dag
                stats-dag
                {:xs [0. 1. 1.2 -1.2]}
                [:mean :THIS-OUTPUT-KEY-DOES-NOT-EXIST]))))))
    (testing "Analysis errors: #'compute"
      (testing "reports validation error as by #'mapdag.analysis/validate"
        (mapdag.test.analysis/test-graph-validation
          (fn [graph input-keys output-keys]
            (call-dag
              graph
              (into {}
                (map (fn [k]
                       [k 1.]))
                input-keys)
              output-keys))))))
  :done)
