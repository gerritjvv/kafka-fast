(ns kafka-clj.producer-tests
  (:require [kafka-clj.producer :refer [write-produce-request]])
  (:use midje.sweet)
  (:import [io.netty.buffer Unpooled ByteBufUtil]))


(facts "Test producer"
       ;[^ByteBuf bytebuf version-id correlation-id client-id required-acks ack-timeout-ms msgs]
       (fact "test write to"
             (let [buff (Unpooled/buffer 100 (Integer/MAX_VALUE))
                   
                  ; (defn ^ByteBuf write-produce-request [^ByteBuf buff codec partition msgs {:keys [acks timeout] :or {acks 2 timeout 500}}]

                   buff2 (write-produce-request buff "snappy" 1 [{:topic "abc" :partition 1 :bts (.getBytes "msg1") } {:topic "b" :partition 1 :bts (.getBytes "msg2")}] {})]
               
               (prn (ByteBufUtil/hexDump buff2))
               (prn (str (java.util.Arrays/toString (.array buff))))
               1 => 1
               
             )))