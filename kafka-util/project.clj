(defproject kafka-util "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :global-vars {*warn-on-reflection* true}

  :aot [kafka-util.core]
  :main kafka-util.core
  :dependencies [
                  [kafka-clj "2.0.3"]
                  [org.clojure/tools.cli "0.3.1"]
                  [org.clojure/clojure "1.6.0"]])
