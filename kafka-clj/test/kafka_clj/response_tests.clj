(ns kafka-clj.response-tests
  (:require [kafka-clj.response :refer :all]
            [clojure.pprint :refer [pprint]])
  (:import [java.net InetAddress]
           [java.nio ByteBuffer]
           [io.netty.handler.codec ReplayingDecoder]
           [io.netty.buffer ByteBuf Unpooled]
           [java.nio.channels SocketChannel]
           [java.net InetSocketAddress]
           [kafka_clj.util Util]
           [java.util ArrayList])
  (:use midje.sweet))

(fact "Test response code"
      
      (fact "Test produce response"
            
            (let [buff (Unpooled/buffer 1024)
                  decoder (produce-response-decoder)
                  out (ArrayList.)
                  ]
              (-> buff
                (.writeInt (int 10))      ;request size
                (.writeInt (int 100))     ;correlation id
                (.writeInt (int 1))       ;topic count
                (.writeShort (short 1))   ;write topic name len
                (.writeBytes (.getBytes "a"))
                (.writeInt   (int 1))     ;write partition count
                (.writeInt   (int 2))     ;write partition
                (.writeShort (short -1))     ;write error code
                (.writeLong  0) )          ;write offset
              
              (.decode decoder nil buff out)
              (let [msg (.get out 0)]
                (pprint msg)
                (count msg) => 1
                (doseq [topic msg]
		                (:topic topic) => "a"
		                (count (:partitions topic)) => 1
		                (doseq [partition (:partitions topic)]
		                  (:partition partition) => 2
		                  (:error-code partition) => -1
		                  (:offset partition) => 0
		                  ))))))