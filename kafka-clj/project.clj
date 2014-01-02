(defproject kafka-clj "0.1.4-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :warn-on-reflection true
  :global-vars {*warn-on-reflection* true
                *assert* false}

  :aot [kafka-clj.client]
  :main kafka-clj.client
  :java-source-paths ["java"]   
  :plugins [
         [lein-rpm "0.0.5"] [lein-midje "3.0.1"] [lein-marginalia "0.7.1"] 
         [lein-kibit "0.0.8"] [no-man-is-an-island/lein-eclipse "2.0.0"]
           ]
  :dependencies [
                 [midje "1.6.0" :scope "test"]
                 [reply "0.3.0" :scope "provided"]
                 [org.clojure/tools.trace "0.7.6"]
                 [org.iq80.snappy/snappy "0.3"]
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-tcp "0.2.1-SNAPSHOT"]
                 [fmap-clojure "0.1.1"]
                 [fun-utils "0.2.4"]
                 [clj-tuple "0.1.4"]
                 [clj-json "0.5.3"]
                 [com.netflix.curator/curator-framework "1.3.3"]
                 [org.clojure/clojure "1.5.1" :scope "provided"]])
