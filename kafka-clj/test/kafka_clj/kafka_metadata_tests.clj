(ns
  ^{:doc
    "
      Test metadata reading from kafka brokers.
      "
    }
  kafka-clj.kafka-metadata-tests

  (:require
    [kafka-clj.metadata :as kafka-meta]

    [kafka-clj.test-utils :as test-utils]
    [clojure.test :refer :all]
    [schema.core :as s]))



;;validates metadata responses like {"abc" [{:host "localhost", :port 50738, :isr [{:host "localhost", :port 50738}], :id 0, :error-code 0}]}
(def META-RESP-SCHEMA kafka-meta/META-RESP-SCHEMA)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; test functions

(defn simple-get-meta-test [resources]

  (let [topics ["abc", "edf", "123"]
        _ (do

            (test-utils/create-topics resources topics 1 1))

        connector (kafka-meta/connector (test-utils/conf-brokers resources) {})
        metadata (kafka-meta/get-metadata connector {})]

    (kafka-meta/close connector)
    (and
      (= (into #{} (keys metadata)) (into #{} topics))
      (s/validate META-RESP-SCHEMA metadata))

    ))


(defn all-brokers-down-and-up-test [resources]
  (let [topics ["abc", "edf", "123"]
        _ (do

            (test-utils/create-topics resources topics 1 1))

        connector (kafka-meta/connector (test-utils/conf-brokers resources) {})

        ;;startup extra nodes
        _ (test-utils/add-kafka-node resources)

        ;;shutdown all previous nodes
        broker1 (test-utils/shutdown-random-kafka resources)
        broker2 (test-utils/shutdown-random-kafka resources)


        resp1 (try
                (kafka-meta/update-metadata! connector {})
                (catch Exception e e))


        _ (test-utils/startup-broker broker1)
        _ (test-utils/startup-broker broker2)

        resp2 (try
                (kafka-meta/update-metadata! connector {})
                (catch Exception e e))


        brokers @(:brokers-ref connector)]

    (kafka-meta/close connector)

    (prn "hosts " (into #{} (test-utils/conf-brokers resources)))
    (prn "got " brokers)
    (prn "resp1 " resp1)
    (prn "resp2" resp2)
    (and

      (or (instance? Throwable resp1) (s/validate META-RESP-SCHEMA resp1))
      (or (instance? Throwable resp1) (s/validate META-RESP-SCHEMA resp2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; unit framework


(defonce TEST-DEFS {:TEST-SIMPLE-GET-META  simple-get-meta-test
                    :TEST-ALL-BROKERS-DOWN all-brokers-down-and-up-test
                    })

(defn run-single-test [f]
  (test-utils/with-resources

    (fn [] (test-utils/startup-resources 2))
    test-utils/shutdown-resources
    f))


(deftest test-simple-get-meta-test []
                                   simple-get-meta-test)

(deftest test-all-browkers-down-and-up-test []
                                            all-brokers-down-and-up-test)

