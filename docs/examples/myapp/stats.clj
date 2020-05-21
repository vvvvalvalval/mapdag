(ns myapp.stats)

(def stats-dag
  {::N
   {:mapdag.step/deps [::raw-numbers]
    :mapdag.step/compute-fn
    (fn [raw-numbers]
      (count raw-numbers))}

   ::sum
   {:mapdag.step/deps [::raw-numbers]
    :mapdag.step/compute-fn
    (fn [raw-numbers]
      (apply + 0. raw-numbers))}

   ::mean
   {:mapdag.step/deps [::sum ::N]
    :mapdag.step/compute-fn
    (fn [sum N] (/ sum N))}

   ::sum-of-squares
   {:mapdag.step/deps [::raw-numbers]
    :mapdag.step/compute-fn
    (fn [raw-numbers]
      (apply + 0.
        (map #(* % %)
          raw-numbers)))}

   ::variance
   {:mapdag.step/deps [::sum-of-squares ::N ::mean]
    :mapdag.step/compute-fn
    (fn [s2 N m]
      (-
        (/ s2 N)
        (* m m)))}})




(require '[mapdag.runtime.default :as mdg-run])

(mdg-run/compute
  stats-dag
  {::raw-numbers [-1. 0. 2. 3.]}
  [::N ::mean])



(require '[mapdag.step :as mdg])

(def stats-dag
  {::N (mdg/step [::raw-numbers]
         (fn [raw-numbers]
           (count raw-numbers)))
   ::sum (mdg/step [::raw-numbers]
           (fn [raw-numbers]
             (apply + 0. raw-numbers)))
   ::mean (mdg/step [::sum ::N]
            (fn [sum N] (/ sum N)))
   ::sum-of-squares (mdg/step [::raw-numbers]
                      (fn [raw-numbers]
                        (apply + 0.
                          (map #(* % %)
                            raw-numbers))))
   ::variance (mdg/step [::sum-of-squares ::N ::mean]
                (fn [s2 N m]
                  (-
                    (/ s2 N)
                    (* m m))))})



(require '[mapdag.runtime.jvm-eval])

(def compute-stats
  (mapdag.runtime.jvm-eval/compile-graph {} stats-dag))

(compute-stats
  {::raw-numbers [-1. 0. 2. 3.]}
  [::N ::mean])
;=> #:myapp.stats{:N 4, :mean 1.0}
