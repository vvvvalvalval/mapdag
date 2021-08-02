(ns mapdag.test.error
  (:require [clojure.test :refer :all]
            [mapdag.error]))


(defn error-api-data
  [err]
  (if (some? err)
    (-> err
      ex-data
      (select-keys mapdag.error/error-keys))
    {::didnt-throw true}))


(defmacro catch-error-api-data
  [expr]
  `(try
     ~expr
     nil
     (catch #?(:clj Throwable :cljs :default) err#
       (error-api-data err#))))
