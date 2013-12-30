(ns kafka-clj.response-tests
  (:require [kafka-clj.response :refer :all]
           
            [kafka-clj.metadata :refer [convert-metadata-response]]
            [clojure.pprint :refer [pprint]])
  (:import [java.net InetAddress]
           [java.nio ByteBuffer]
           [io.netty.handler.codec ReplayingDecoder]
           [io.netty.buffer ByteBuf Unpooled]
           [java.nio.channels SocketChannel]
           [java.net InetSocketAddress]
           [kafka_clj.util Util]
           [java.util ArrayList])
  (:use midje.sweet
        clojure.data))

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
		                  )))))
      
      (fact "Test metadata response"
            (let [buff (Unpooled/buffer 1024)
                  decoder (metadata-response-decoder)
                  out (ArrayList.)]
              
              (-> buff 
                (.writeInt (int 10))     ;request size
                (.writeInt (int 2))      ;write correlation id 
                (.writeInt (int 1))      ;broker count
                (.writeInt (int 0))      ;broker 0
                (.writeShort (short 1))  ;broker host len
                (.writeBytes (.getBytes "a")) ;host
                (.writeInt (int 9092))        ;port
                (.writeInt (int 1))           ;metadata array count
                (.writeShort (short 0))       ;errror code
                (.writeShort (short 1))       ;topic name len
                (.writeBytes (.getBytes "p")) ;topic name
                (.writeInt (int 1))           ;partition metadata array count
                (.writeShort (short 0))       ;error code
                (.writeInt (int 0))           ;partition id
                (.writeInt (int 0))           ;leader id
                (.writeInt (int 2))           ;replica array count
                (.writeInt (int 0))           ;replica 0
                (.writeInt (int 1))           ;replica 1
                (.writeInt (int 1))           ;isr array count
                (.writeInt (int 0)))           ;isr
                
              (.decode decoder nil buff out)
              (let [msg (.get out 0)]
                (pprint msg)
                (count msg) => 3
                (let [d (diff msg {:correlation-id 2,
												 :brokers [{:node-id 0, :host "a", :port 9092}],
												 :topics
												 [{:error-code 0,
												   :topic "p",
												   :partitions
												   [{:partition-error-code 0,
												     :partition-id 0,
												     :leader 0,
												     :replicas '(0 1),
												     :isr '(0)}]}]})]
                  (first d) => nil
                  (second d) => nil)
                
                
                  ;;test convert
                  (prn "Convert")
                  (pprint (convert-metadata-response msg))
                  (let [d (diff (convert-metadata-response msg) {"p" [{:host "a" :port 9092}]})]
                    (first d) => nil
                    (second d) => nil)
                
                )
												                
                  
            )))