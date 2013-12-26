(ns kafka-clj.producer-tests
  (:require [kafka-clj.producer :refer [write-request  read-produce-request write-produce-request]]
            [clojure.pprint :refer [pprint]])
  (:use midje.sweet)
  (:import [io.netty.buffer Unpooled ByteBufUtil]))


(facts "Test producer"
       ;[^ByteBuf bytebuf version-id correlation-id client-id required-acks ack-timeout-ms msgs]
       (fact "test write to"
             (let [buff (Unpooled/buffer 100 (Integer/MAX_VALUE))
                   
                  ; (defn ^ByteBuf write-produce-request [^ByteBuf buff codec partition msgs {:keys [acks timeout] :or {acks 2 timeout 500}}]

                  ;(defn ^ByteBuf write-request [^ByteBuf buff correlation-id client-id codec partition request-msg-f conf msgs]
                  ;write-reques[^ByteBuf buff correlation-id client-id codec partition request-msg-f conf msgs]
                   buff2 (write-request buff 1 "client" "none" 1 write-produce-request {} [{:topic "abc" :partition 1 :bts (.getBytes "msg1") } {:topic "b" :partition 0 :bts (.getBytes "msg2")}])]
               
               (pprint (read-produce-request buff2))
               
               1 => 1
               
             )))