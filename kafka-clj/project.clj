(defproject kafka-clj "4.0.2-SNAPSHOT"
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

  :profiles {:test {:jvm-opts ["-Xmx1g" "-server"

                               ;;------- Properties for kerberos authentication
                               ;"-Dsun.security.krb5.debug=true"
                               ;"-Djava.security.debug=gssloginconfig,configfile,configparser,logincontext"
                               ;"-Djava.security.auth.login.config=/vagrant/vagrant/config/kafka_client_jaas.conf"
                               ;"-Djava.security.krb5.conf=/vagrant/vagrant/config/krb5.conf"
                               ]}

             :uberjar {:aot :all}

             :repl {:jvm-opts [
                               "-Xmx512m"
                               "-Dsun.security.krb5.debug=true"
                               "-Djava.security.debug=gssloginconfig,configfile,configparser,logincontext"
                               "-Djava.security.auth.login.config=/vagrant/vagrant/config/kafka_client_jaas.conf"
                               "-Djava.security.krb5.conf=/vagrant/vagrant/config/krb5.conf"
                               ]}

             :vagrant-digital {
                               :jvm-opts [
                                          ;"-Dcom.sun.management.jmxremote.port=8855"
                                          "-Dcom.sun.management.jmxremote.authenticate=false"
                                          "-Dcom.sun.management.jmxremote.ssl=false"
                                          "-server" "-Xms8g" "-Xmx8g"]
                               :main kafka-clj.app
                               }
             }

  :main kafka-clj.app

  :plugins [[lein-midje "3.2.1"]
            [lein-kibit "0.0.8"]]

  :test-paths ["test" "test-java"]

  :dependencies [
                 [tcp-driver "0.1.2-SNAPSHOT"]

                 [com.taoensso/carmine "2.15.0" :exclusions [org.clojure/clojure]]
                 [org.redisson/redisson "3.2.2" :exclusions [io.netty/netty-buffer]]

                 [com.alexkasko.unsafe/unsafe-tools "1.4.4"]

                 [criterium "0.4.4" :scope "test"]

                 [prismatic/schema "1.1.3"]
                 [org.mapdb/mapdb "1.0.9"]
                 [org.xerial.snappy/snappy-java "1.1.2.4"]

                 [net.jpountz.lz4/lz4 "1.3.0"]
                 [org.clojure/tools.logging "0.3.1"]

                 [fun-utils "0.6.1" :exclusions [org.clojure/tools.logging]]
                 [clj-tuple "0.2.2"]

                 [io.netty/netty-buffer "4.1.6.Final"]
                 [io.netty/netty-common "4.1.6.Final"]

                 [com.codahale.metrics/metrics-core "3.0.2"]

                 [org.clojure/core.async "0.2.374"
                  :exclusions [org.clojure/tools.reader]]

                 [com.stuartsierra/component "0.3.2"]

                 [org.openjdk.jol/jol-core "0.5"]

                 [org.clojure/clojure "1.8.0" :scope "provided"]

                 [org.clojure/test.check "0.9.0" :scope "test"]

                 [midje "1.8.3" :scope "test"
                  :exclusions [potemkin
                               riddley]]

                 [org.clojars.runa/conjure "2.2.0" :scope "test"]

                 [org.apache.zookeeper/zookeeper "3.4.8" :scope "test"
                  :exclusions [io.netty/netty]]
                 [org.apache.kafka/kafka_2.10 "0.10.1.0" :scope "test"
                  :exclusions [io.netty/netty
                               log4j
                               org.slf4j/slf4j-api
                               org.slf4j/slf4j-log4j12]]
                 [com.github.kstyrc/embedded-redis "0.6" :scope "test"]])
