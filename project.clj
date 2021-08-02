(defproject vvvvalvalval/mapdag "0.2.2"
  :description "A library for expressing computations as DAGs of named steps. Maps-in, maps-out. AOT compilation available on the JVM for high-performance scenarios."
  :url "https://github.com/vvvvalvalval/mapdag"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :plugins [[lein-tools-deps "0.4.5"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:project]
                           :aliases []})
