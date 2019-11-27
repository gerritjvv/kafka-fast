(defproject kafka-events-disk "0.2.3-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :warn-on-reflection true
  :global-vars {*warn-on-reflection* true
                *assert* false}

  :scm {:name "git"
        :url "https://github.com/gerritjvv/kafka-fast.git"}
  :java-source-paths ["java"]
  :jvm-opts ["-Xmx3g" "--add-modules" "java.xml.bind”]
  :plugins [
             [lein-midje "3.0.1"] [lein-marginalia "0.7.1"]
             [lein-cloverage "1.0.2"]
             [lein-kibit "0.0.8"]
             ]
  :dependencies [
                  [fileape "0.6.3"]
                  [midje "1.6.0" :scope "test"]
                  [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                  [org.clojure/data.json "0.2.5"]
                  [org.apache.hadoop/hadoop-common "2.2.0" :scope "provided"]
                  [org.clojure/clojure "1.6.0"]])
