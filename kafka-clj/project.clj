(defproject kafka-clj "1.0.0-SNAPSHOT"
  :description "fast kafka library implemented in clojure"
  :url "https://github.com/gerritjvv/kafka-fast"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  ;:warn-on-reflection true
  ;:global-vars {*warn-on-reflection* true
   ;             *assert* false}

  :aot [kafka-clj.client]
  :main kafka-clj.client
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
                 [group-redis "0.6.0-SNAPSHOT"]
                 [org.mapdb/mapdb "0.9.9"]
                 [midje "1.6.0" :scope "test"]
                 [reply "0.3.0" :scope "provided"]
                 [org.clojure/tools.trace "0.7.6"]
                 [org.xerial.snappy/snappy-java "1.1.1-M1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-tcp "0.4.0"]
                 [fmap-clojure "LATEST"]
                 [fun-utils "LATEST"]
                 [clj-tuple "0.1.4"]
                 [com.codahale.metrics/metrics-core "3.0.1"]
                 
                
                ; [org.apache.kafka/kafka_2.10 "0.8.0" :scope "test" :exclusions [[javax.mail/mail :extension "jar"]
                 ;                             [javax.jms/jms :classifier "*"]
                  ;                            com.sun.jdmk/jmxtools
                   ;                           com.sun.jmx/jmxri]]
                 ;[org.apache.curator/curator-test "2.3.0" :scope "test" ]
                 ;[org.apache.hadoop/hadoop-core "0.20.2"]
                 [org.clojure/clojure "1.6.0" :scope "provided"]])
