(ns mapdag.test.analysis
  (:require [clojure.test :refer :all]
            [mapdag.step :as mdg]
            [mapdag.analysis]
            [mapdag.test.error]))


(def graph-with-deps-cycle
  {:a (mdg/step [:c0 :input-0] (constantly :a))
   :b (mdg/step [:c2] (constantly :b))

   :c0 (mdg/step [:c1] (constantly :c0))
   :c1 (mdg/step [:c2 :c1-dep] (constantly :c1))
   :c2 (mdg/step [:c0] (constantly :c2))

   :c1-dep (mdg/step [:input-1] (constantly :c1-dep))

   :d (mdg/step [:e0 :e1] (constantly :d))
   :e0 (mdg/step [:input-0] (constantly :e0))
   :e1 (mdg/step [:input-0] (constantly :e1))})


(defn reports-error-involving-step?
  [err-reason ks err-data]
  (and
    (-> err-data :mapdag.error/reason
      (= err-reason))
    (contains? (set ks)
      (:mapdag.step/name err-data))))


(defn reports-deps-cycle-involving?
  [ks err-data]
  (reports-error-involving-step? :mapdag.errors/dependency-cycle
    ks err-data))


(defn test-graph-validation 
  [validate-fn]
  (testing "Dependency validation"
    (testing "reports missing inputs or steps"
      (is
        (reports-error-involving-step? :mapdag.errors/missing-step-or-input
          #{:input-0}
          (mapdag.test.error/catch-error-api-data
            (validate-fn
              (select-keys graph-with-deps-cycle
                #{:d :e0 :e1})
              #{}
              #{:d}))))
      (is
        (reports-error-involving-step? :mapdag.errors/missing-step-or-input
          #{:THIS-OUTPUT-KEY-DOES-NOT-EXIST}
          (mapdag.test.error/catch-error-api-data
            (validate-fn
              (select-keys graph-with-deps-cycle
                #{:d :e0 :e1})
              #{:input-0}
              [:THIS-OUTPUT-KEY-DOES-NOT-EXIST])))))
    (testing "reports dependency cycles"
      (is
        (reports-deps-cycle-involving? #{:c0 :c1 :c2}
          (mapdag.test.error/catch-error-api-data
            (validate-fn graph-with-deps-cycle
              #{:input-0 :input-1}
              [:b]))))
      (testing "when not broken by inputs"
        (validate-fn graph-with-deps-cycle
          #{:c1 :input-0 :input-1}
          [:b]))
      (testing "when not outside of the required subgraph"
        (validate-fn graph-with-deps-cycle
          #{:input-0}
          [:d])))))


(deftest validate-graph-validates-with-inputs-and-outputs--examples 
  (test-graph-validation mapdag.analysis/validate-graph))


(deftest validate-graph--examples
  (testing "Dependency validation"
    (testing "reports missing inputs or steps"
      (is
        (reports-error-involving-step? :mapdag.errors/missing-step-or-input
          #{:input-0}
          (mapdag.test.error/catch-error-api-data
            (mapdag.analysis/validate-graph
              (select-keys graph-with-deps-cycle
                #{:d :e0 :e1})
              #{}
              false))))
      (is
        (reports-error-involving-step? :mapdag.errors/missing-step-or-input
          #{:input-0}
          (mapdag.test.error/catch-error-api-data
            (mapdag.analysis/validate-graph
              (select-keys graph-with-deps-cycle
                #{:d :e0 :e1})
              #{}
              #{:d}))))
      (is
        (reports-error-involving-step? :mapdag.errors/missing-step-or-input
          #{:THIS-OUTPUT-KEY-DOES-NOT-EXIST}
          (mapdag.test.error/catch-error-api-data
            (mapdag.analysis/validate-graph
              (select-keys graph-with-deps-cycle
                #{:d :e0 :e1})
              #{:input-0}
              [:THIS-OUTPUT-KEY-DOES-NOT-EXIST])))))
    (testing "reports dependency cycles"
      (is
        (reports-deps-cycle-involving? #{:c0 :c1 :c2}
          (mapdag.test.error/catch-error-api-data
            (mapdag.analysis/validate-graph graph-with-deps-cycle
              false false))))
      (is
        (reports-deps-cycle-involving? #{:c0 :c1 :c2}
          (mapdag.test.error/catch-error-api-data
            (mapdag.analysis/validate-graph graph-with-deps-cycle
              #{:input-0 :input-1}
              false))))
      (is
        (reports-deps-cycle-involving? #{:c0 :c1 :c2}
          (mapdag.test.error/catch-error-api-data
            (mapdag.analysis/validate-graph graph-with-deps-cycle
              #{:input-0 :input-1}
              [:b]))))
      (testing "when not broken by inputs"
        (mapdag.analysis/validate-graph graph-with-deps-cycle
          #{:c1 :input-0 :input-1}
          false)
        (mapdag.analysis/validate-graph graph-with-deps-cycle
          #{:c1 :input-0 :input-1}
          [:b]))
      (testing "when not outside of the required subgraph"
        (mapdag.analysis/validate-graph graph-with-deps-cycle
          #{:input-0}
          [:d])))))

