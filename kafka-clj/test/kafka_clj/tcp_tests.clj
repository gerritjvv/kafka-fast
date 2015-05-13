(ns kafka-clj.tcp-tests
  (:require [kafka-clj.tcp :as tcp]
            [clj-tcp.echo-server :as server])
  (:use midje.sweet))


;;; Test the kafka-clj.tcp connections

(def test-facts (ref {}))

(defn start-server []
  (server/start-server 7009))

(defn start-test-facts! []
  (dosync
    (alter test-facts (fn [m]
                        (assoc m :server (start-server))))))

(defn stop-test-facts! []
  (server/close-server (get @test-facts :server)))

(with-state-changes
  [(before :facts (start-test-facts!))
   (after  :facts (stop-test-facts!))]

  (fact "Send byte array data"
        (let [data (promise)
              client (tcp/tcp-client "localhost" 7009)
              f (tcp/read-async-loop! client (fn [v] (deliver data (String. ^"[B" v))))]

          (tcp/write! client (.getBytes "hi") :flush true)
          @data => "hi"

          (tcp/close! client)))

  (fact "Test Pool Send byte array data"
        (let [data (promise)
              pool (tcp/tcp-pool {:max-total 2})
              client (tcp/borrow pool "localhost" 7009)
              f (tcp/read-async-loop! client (fn [v] (deliver data (String. ^"[B" v))))]

          (tcp/write! client (.getBytes "hi") :flush true)
          @data => "hi"
          (tcp/release pool "localhost" 7009 client)
          (tcp/close-pool! client))))