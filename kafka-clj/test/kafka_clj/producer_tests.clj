(ns kafka-clj.producer-tests
  (:require [kafka-clj.producer :refer [write-to-buff!]])
  (:use midje.sweet)
  (:import [io.netty.buffer Unpooled ByteBufUtil]))


(facts "Test producer"
       ;[^ByteBuf bytebuf version-id correlation-id client-id required-acks ack-timeout-ms msgs]
       (fact "test write to"
             (let [buff (Unpooled/buffer 100 (Integer/MAX_VALUE))
                   buff2 (write-to-buff! buff 1 2 "hi" 2 1000 [{:topic "a" :partition 1 :bts (.getBytes "msg1") } {:topic "b" :partition 1 :bts (.getBytes "msg2")}])]
               
               (prn (ByteBufUtil/hexDump buff2))
               1 => 1
               
             )))