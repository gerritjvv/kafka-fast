(defproject kafka-clj "3.6.6"
  :description "fast kafka library implemented in clojure"
  :url "https://github.com/gerritjvv/kafka-fast"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]

  :global-vars {*warn-on-reflection* true
               *assert* true}

  :scm {:name "git"
         :url "https://github.com/gerritjvv/kafka-fast.git"}
  :java-source-paths ["java"]
  :jvm-opts ["-Xmx3g" "-server"]
  :plugins [[lein-midje "3.1.1"]
            [lein-kibit "0.0.8"]]
  :test-paths ["test" "test-java"]
  :dependencies [
                 [com.taoensso/carmine "2.12.2" :exclusions [org.clojure/clojure]]
                 [org.redisson/redisson "2.2.16" :exclusions [io.netty/netty-buffer]]

                 [com.alexkasko.unsafe/unsafe-tools "1.4.4"]

                 [criterium "0.4.4" :scope "test"]
                 [org.mapdb/mapdb "1.0.9"]
                 [org.xerial.snappy/snappy-java "1.1.2.4"]

                 [net.jpountz.lz4/lz4 "1.3.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-tcp "1.0.1"
                  :exclusions [com.taoensso/nippy
                               com.taoensso/truss
                               org.clojure/tools.reader
                               com.taoensso/encore]]

                 [fun-utils "0.6.1" :exclusions [org.clojure/tools.logging]]
                 [clj-tuple "0.2.2"]

                 [com.codahale.metrics/metrics-core "3.0.2"]

                 [org.clojure/core.async "0.2.374"
                  :exclusions [org.clojure/tools.reader]]

                 [com.stuartsierra/component "0.3.1"]

                 [org.openjdk.jol/jol-core "0.5"]

                 [org.clojure/clojure "1.8.0" :scope "provided"]

                 [org.clojure/test.check "0.9.0" :scope "test"]

                 [midje "1.8.3" :scope "test"
                  :exclusions [potemkin
                               riddley]]

                 [org.clojars.runa/conjure "2.2.0" :scope "test"]

                 [org.apache.zookeeper/zookeeper "3.4.8" :scope "test"
                  :exclusions [io.netty/netty]]
                 [org.apache.kafka/kafka_2.10 "0.10.0.0" :scope "test"
                  :exclusions [io.netty/netty
                               log4j
                               org.slf4j/slf4j-api
                               org.slf4j/slf4j-log4j12]]
                 [com.github.kstyrc/embedded-redis "0.6" :scope "test"]])
