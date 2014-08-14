(defproject kafka-clj "2.3.2"
  :description "fast kafka library implemented in clojure"
  :url "https://github.com/gerritjvv/kafka-fast"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :warn-on-reflection true
  :global-vars {*warn-on-reflection* true
                *assert* false}

  :scm {:name "git"
         :url "https://github.com/gerritjvv/kafka-fast.git"}
  :java-source-paths ["java"]  
  :jvm-opts ["-Xmx3g"]
  :plugins [
         [lein-rpm "0.0.5"] [lein-midje "3.0.1"] [lein-marginalia "0.7.1"]
	       [lein-cloverage "1.0.2"]
         [lein-kibit "0.0.8"] [no-man-is-an-island/lein-eclipse "2.0.0"]
           ]
  :dependencies [
                 [org.clojars.smee/binary "0.2.5"]
                 [clojurewerkz/buffy "1.0.0-beta1"]
                 [group-redis "0.6.3"]
                 [org.mapdb/mapdb "0.9.9"]
                 [midje "1.6.0" :scope "test"]
                 ;[reply "0.3.0" :scope "provided"]
                 [org.clojure/tools.trace "0.7.6"]
                 [org.xerial.snappy/snappy-java "1.1.1-M1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-tcp "0.4.0"]
                 [org.clojure/data.json "0.2.5"]
                 [fmap-clojure "LATEST"]
                 [fun-utils "LATEST"]
                 [clj-tuple "0.1.4"]
                 [thread-load "0.1.1"]
                 [com.codahale.metrics/metrics-core "3.0.1"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [org.clojure/clojure "1.5.1" :scope "provided"]])
