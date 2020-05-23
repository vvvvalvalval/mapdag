(ns mapdag.dev
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [fipp.clojure]))

(defn- shorten-form
  [code]
  (let [current-ns-str (name (ns-name *ns*))]
    (letfn [(aux [code]
              (cond
                (vector? code) (mapv aux code)
                (sequential? code) (apply list (map aux code))
                (map? code) (into {}
                              (map
                                (fn [[k v]]
                                  [(aux k)
                                   (aux v)]))
                              code)
                (symbol? code) (let [sns (namespace code)]
                                 (if (or
                                       (contains? #{"clojure.core"} sns)
                                       (= sns current-ns-str))
                                   (symbol (name code))
                                   (if-some [m (re-matches
                                                 #"(java\.lang\.|java\.util\.|clojure\.lang\.)([^\.]+\.?)"
                                                 (name code))]
                                     (let [[_ _prefix suffix] m]
                                       (symbol suffix))
                                     code)))
                :other code))]
      (aux code))))


(defn pprint-generated-code
  [expr]
  (-> expr
    (shorten-form)
    (fipp.clojure/pprint)))
