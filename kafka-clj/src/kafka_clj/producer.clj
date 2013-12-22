(ns kafka-clj.producer
  
  (:import 
           [io.netty.buffer ByteBuf]))


(defn ^ByteBuf inc-capacity [^ByteBuf bytebuf l]
  (.capacity bytebuf (int (+ (.capacity bytebuf) (int l)))))

(defn ^ByteBuf write-short-string [^ByteBuf buff s]
  (if 
    (.writeShort buff (short -1))
    (let [encoded-string (.getBytes (str s) "UTF-8")
          len (count encoded-string)]
      (if (> len Short/MAX_VALUE)
        (throw (RuntimeException. (str "String exceeds the maximum size of " Short/MAX_VALUE)))
        (doto buff 
          (.writeShort len) (.writeBytes encoded-string)))))
  buff) 

(defn ^ByteBuf write-to-buff! [^ByteBuf bytebuf version-id correlation-id client-id required-acks ack-timeout-ms msgs]
  "Writes the messages to the bytebuf, note that this function mutates the bytebuf
   msgs is a list of maps/records that contain the values [:topic string-topic-name] [:partition int-partition] [:bts byte-array] 
  "
  (let [grouped-msg (group-by :topic msgs)
			  byte-buf1 (-> (inc-capacity bytebuf (+ 18 (count client-id)))
									    (.writeShort (short version-id))
									    (.writeInt (int correlation-id))
									    (write-short-string client-id)
									    (.writeShort (short required-acks))
									    (.writeInt (int ack-timeout-ms)))]
    (loop [topic-msgs grouped-msg byte-buf2 byte-buf1]
        (if-let [[topic msgs] (first topic-msgs)]
				      (recur
                    (rest topic-msgs)
				            (loop [msgs-1 msgs byte-buf3 (-> (inc-capacity byte-buf2 (+ 6 (count topic))) (write-short-string topic) (.writeInt (count msgs)))]
								        (if-let [msg (first msgs-1)]
								          (recur (rest msgs-1)
								                 (-> (inc-capacity byte-buf3 (+ 8 (count (:bts msg))))
								                   (.writeInt (int (:partition msg)))
								                   (.writeInt (int (count (:bts msg))))
								                   (.writeBytes ^bytes (:bts msg))))
								          byte-buf3)))
              byte-buf2))))
		                  
		      
      
		    
      
      
      