(defproject kafka-mon "0.1.0-SNAPSHOT"
  :description "Utilities for monitor kafka 0.8.0"
  :url "https://github.com/gerritjvv/kafka-fast"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :aot [kafka-mon.core]
  :main kafka-mon.core
  :jvm-opts ["-Xmx3g"]
  :plugins [
         [lein-rpm "0.0.5"] [lein-midje "3.0.1"] [lein-marginalia "0.7.1"]
	 [lein-cloverage "1.0.2"]  
         [lein-kibit "0.0.8"] [no-man-is-an-island/lein-eclipse "2.0.0"]
           ]

  :dependencies [
                 [kafka-clj "0.9.3"]
                 [org.clojure/tools.cli "0.3.0"]
                 [org.clojure/clojure "1.5.1"]
                 [incanter/incanter-core "1.5.5"]])
