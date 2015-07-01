(defproject kafka-clj "3.1.2-SNAPSHOT"
  :description "fast kafka library implemented in clojure"
  :url "https://github.com/gerritjvv/kafka-fast"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]

  :global-vars {*warn-on-reflection* true
               *assert* false}

  ;:main kafka-clj.app
  :scm {:name "git"
         :url "https://github.com/gerritjvv/kafka-fast.git"}
  :java-source-paths ["java"]
  :jvm-opts ["-Xmx3g"]
  :plugins [
         [lein-rpm "0.0.5"] [lein-midje "3.0.1"] [lein-marginalia "0.7.1"]
	       [lein-cloverage "1.0.2"]
         [lein-kibit "0.0.8"] [no-man-is-an-island/lein-eclipse "2.0.0"]
           ]
  :test-paths ["test" "test-java"]
  :dependencies [
                 [org.clojars.runa/conjure "2.1.3" :scope "test"]

                 [com.taoensso/carmine "2.7.0"]
                 [redis.clients/jedis "2.6.2"]
                 [org.redisson/redisson "1.2.1"]
                 [org.apache.commons/commons-pool2 "2.2"]
                 [com.alexkasko.unsafe/unsafe-tools "1.4.4"]

                 [org.mapdb/mapdb "1.0.6"]
                 [midje "1.6.3" :scope "test"]
                 [org.clojure/tools.trace "0.7.6"]
                 [org.xerial.snappy/snappy-java "1.1.1.6"]

                 [net.jpountz.lz4/lz4 "1.3.0"]
                 [org.clojure/tools.logging "0.3.0"]
                 [clj-tcp "0.4.9-SNAPSHOT"]
                 [fmap-clojure "LATEST" :exclusions [org.clojure/tools.logging]]
                 [fun-utils "0.5.8-SNAPSHOT" :exclusions [org.clojure/tools.logging]]
                 [clj-tuple "0.1.7"]
                 [thread-load "0.2.0-SNAPSHOT" :exclusions [org.clojure/clojure]]
                 [com.codahale.metrics/metrics-core "3.0.1"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [com.stuartsierra/component "0.2.2"]
                 [org.clojure/clojure "1.6.0" :scope "provided"]
                 [org.apache.zookeeper/zookeeper "3.4.6" :scope "provided"
                  :exclusions [io.netty/netty]]
                 [org.apache.kafka/kafka_2.10 "0.8.1.1" :scope "provided"
                  :exclusions [io.netty/netty]]
                 [redis.embedded/embedded-redis "0.3" :scope "provided"]
                 ])
