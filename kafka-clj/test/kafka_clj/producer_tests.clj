(ns kafka-clj.producer-tests
  (:require [kafka-clj.producer :refer [send-messages producer]]
            [clojure.pprint :refer [pprint]])
  (:use midje.sweet)
  (:import [io.netty.buffer Unpooled ByteBufUtil]))


(facts "Test producer"
       ;[^ByteBuf bytebuf version-id correlation-id client-id required-acks ack-timeout-ms msgs]
       (fact "test write to"
             (let [
                   p (producer "localhost" 9092)
                   d [{:topic "data" :partition 0 :bts (.getBytes "HI1")} {:topic "data" :partition 0 :bts (.getBytes "ho4")}] ]
               
               (send-messages p {:acks 1 :codec 1} d)
               (send-messages p {:acks 1 :codec 2} d)
               (send-messages p {:acks 1 :codec 0} d)
               
               
               )))
             