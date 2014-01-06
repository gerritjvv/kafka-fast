(ns kafka-clj.fetch
  (:require [clojure.tools.logging :refer [error info]]
            [clj-tuple :refer [tuple]]
            [clj-tcp.codec :refer [default-encoder]]
            [clj-tcp.client :refer [client write! read! close-all ALLOCATOR read-print-ch read-print-in]]
            [fun-utils.core :refer [fixdelay apply-get-create]]
            [kafka-clj.offset-storage :as offset-storage]
            [kafka-clj.codec :refer [uncompress]]
            [kafka-clj.metadata :refer [track-broker-partitions]]
            [kafka-clj.buff-utils :refer [write-short-string with-size read-short-string read-byte-array codec-from-attributes]]
            [kafka-clj.produce :refer [API_KEY_FETCH_REQUEST API_KEY_OFFSET_REQUEST API_VERSION MAGIC_BYTE]]
            [clojure.core.async :refer [go >! <! chan <!!]])
  (:import [io.netty.buffer ByteBuf Unpooled PooledByteBufAllocator]
           [io.netty.handler.codec ByteToMessageDecoder ReplayingDecoder]
           [java.util List]
           [kafka_clj.util Util]))

(defn ^ByteBuf write-fecth-request-message [^ByteBuf buff {:keys [max-wait-time min-bytes topics max-bytes]
                                          :or { max-wait-time 1000 min-bytes 1 max-bytes 1048576}}]
  "FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]  ReplicaId => int32  MaxWaitTime => int32  MinBytes => int32  TopicName => string  Partition => int32  FetchOffset => int64  MaxBytes => int32"
  (-> buff
    (.writeInt (int -1))
    (.writeInt (int max-wait-time))
    (.writeInt (int min-bytes))
    (.writeInt (int (count topics)))) ;write topic array count
    
   (doseq [[topic partitions] topics]
	    (-> buff
       ^ByteBuf (write-short-string topic)
       (.writeInt (count partitions))) ;write partition array count
      (doseq [{:keys [partition offset]} partitions];default max-bytes 500mb
		    (-> buff
        (.writeInt (int partition))
		    (.writeLong offset)
		    (.writeInt (int max-bytes)))))
   
   buff)
	  

(defn ^ByteBuf write-fetch-request-header [^ByteBuf buff {:keys [client-id correlation-id] :or {client-id "1" correlation-id 11} :as state}]
  "
   RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
  ApiKey => int16
  ApiVersion => int16
  CorrelationId => int32
  ClientId => string
  RequestMessage => FetchRequestMessage
  "
  (-> buff
     (.writeShort (short API_KEY_FETCH_REQUEST))
     (.writeShort (short API_VERSION))
     (.writeInt (int correlation-id))
     ^ByteBuf (write-short-string client-id)
     ^ByteBuf (write-fecth-request-message state)))

(defn ^ByteBuf write-fetch-request [^ByteBuf buff state]
  "Writes a fetch request api call"
    (with-size buff write-fetch-request-header state))


(declare read-message-set)
(declare read-messages0)

(defn read-message [^ByteBuf buff]
  "Message => Crc MagicByte Attributes Key Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  Key => bytes
  Value => bytes"
  (let [crc (Util/unsighedToNumber (.readInt buff))
        magic-byte (.readByte buff)
        attributes (.readByte buff)
        codec (codec-from-attributes attributes)
        key-arr (read-byte-array buff)
        val-arr (read-byte-array buff)]
    ;check attributes, if the message is compressed, uncompress and read messages
    ;else return as is
    (if (> codec 0)
      (let [ ^"[B" ubytes (try (uncompress codec val-arr) (catch Exception e (do   nil)))
             ]
        ;(prn "decompress " codec   " bts " (String. val-arr))
        (if ubytes
          (let [ubuff (Unpooled/wrappedBuffer ubytes)]
	            (doall 
			            ;read the messages inside of this compressed message
			            (mapcat flatten (map :message (read-messages0 ubuff (count ubytes))))))))
      (tuple {:crc crc :key key-arr :bts val-arr}))))
    
        
   
(defn read-message-set [^ByteBuf in]
  "MessageSet => [Offset MessageSize Message]
  Offset => int64
  MessageSize => int32
  Message => Crc MagicByte Attributes Key Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  Key => bytes
  Value => bytes"
       (let [offset (.readLong in)
             message-size (.readInt in)]
		        {:offset offset
		         :message-size message-size
		         :message (read-message in)}))

(defn read-messages0 [^ByteBuf buff message-set-size]
  "Read all the messages till the number of bytes message-set-size have been read"
  (let [reader-start (.readerIndex buff)]
		  (loop [msgs []]
		    (if (< (- (.readerIndex buff) reader-start)  message-set-size)
          (recur (conj msgs (read-message-set buff)))
          msgs))))

(defn read-messages [^ByteBuf buff]
  "Read all the messages till the number of bytes message-set-size have been read"
  (let [message-set-size (.readInt buff)]
    (read-messages0 buff message-set-size)))
            
        

(defn read-fetch-response [^ByteBuf in]
  "
	RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32
	
  Response => CorrelationId ResponseMessage
	CorrelationId => int32
  ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse
  
  FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
  HighwaterMarkOffset => int64
  MessageSetSize => int32
  "

   (let [size (.readInt in)                                ;request size int
				 correlation-id (.readInt in)                      ;correlation id int
				 topic-count (.readInt in)
         
         ]
       
       {:correlation-id correlation-id
        :topics 
		        (doall
		          (for [i (range topic-count)]
		            (let [topic (read-short-string in)
                      partition-count (.readInt in)]
                      [topic 
                       (doall
                         (for [p (range topic-count)]
                          {:partition (.readInt in)
                           :error-code (.readShort in)
                           :high-water-mark-offset (.readLong in)
                           :messages  (read-messages in)
                           }
                           ))])))
        }
     ))

(defn fetch-response-decoder []
  "
   A handler that reads fetch request responses
 
   "
  (proxy [ReplayingDecoder]
    ;decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    []
    (decode [ctx ^ByteBuf in ^List out] 
			      (.add out
					        (read-fetch-response in))
        )))


(defn write-offset-request-message [^ByteBuf buff {:keys [topics max-offsets use-earliest] :or {use-earliest true max-offsets 10}}]
  "
	OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
	  ReplicaId => int32
	  TopicName => string
	  Partition => int32
	  Time => int64
	  MaxNumberOfOffsets => int32
	"
  (.writeInt buff (int -1)) ;replica id
  (.writeInt buff (int (count topics))) ;write topic array count
  (doseq [[topic partitions] topics]
    (write-short-string buff topic)
    (.writeInt buff (int (count partitions)))
    (doseq [{:keys [partition]} partitions]
      (-> buff
        (.writeInt (int partition))
        (.writeLong (if use-earliest -2 -1))
        (.writeInt  (int max-offsets)))))
        
    buff)

 
(defn write-offset-request [^ByteBuf buff {:keys [correlation-id client-id] :or {correlation-id 1 client-id "1"} :as state}]
    (-> buff
      (.writeShort  (short API_KEY_OFFSET_REQUEST))   ;api-key
      (.writeShort  (short API_VERSION))                ;version api
      (.writeInt (int correlation-id))                  ;correlation id
      (write-short-string client-id) 
      (write-offset-request-message state)))


(defn read-offset-response [^ByteBuf in]
  "
  RequestOrResponse => Size (RequestMessage | ResponseMessage)
    Size => int32

   Response => CorrelationId ResponseMessage
    CorrelationId => int32
    ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse

   OffsetResponse => [TopicName [PartitionOffsets]]
	  PartitionOffsets => Partition ErrorCode [Offset]
	  Partition => int32
	  ErrorCode => int16
	  Offset => int64
  "
  (let [size (.readInt in)                                ;request size int
				              correlation-id (.readInt in)                      ;correlation id int
				              topic-count (.readInt in)]                        ;topic array count int
				          (doall ;we must force the operation here
				              (for [i (range topic-count)]                   
						            (let [topic (read-short-string in)]                 ;topic name has len=short string bytes
						              {:topic topic
						               :partitions (let [partition-count (.readInt in)] ;read partition array count int
                                   (doall
						                             (for [q (range partition-count)]
						                               {:partition (.readInt in)        ;read partition int
						                                :error-code (.readShort in)     ;read error code short
						                                :offsets 
                                                    (let [offset-count (.readInt in)]
                                                      (doall
                                                           (for [o (range offset-count)]
                                                             (.readLong in))))}))  )     ;read offset long
						               })))))

(defn offset-response-decoder []
  "
   A handler that reads offset request responses
 
   "
  (proxy [ReplayingDecoder]
    ;decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    []
    (decode [ctx ^ByteBuf in ^List out] 
			      (try (.add out
					        (read-offset-response in))
                (catch Exception e (do 
                                     (.printStackTrace e)
                                     (throw e)
                                     )))
        )))

(defn send-offset-request [{:keys [client conf]} topics]
  "topics must have format [[topic [{:partition 0} {:partition 1}...]] ... ]"
  (write! client (fn [^ByteBuf buff]
                    (with-size buff write-offset-request (merge conf {:topics topics}))))
  
  )

(defn send-fetch [{:keys [client conf]} topics]
  "topics must have format [[topic [{:partition 0} {:partition 1}...]] ... ]"
  (write! client (fn [^ByteBuf buff]
                   (write-fetch-request buff (merge conf {:topics topics}))))
  )


(defn flip-sent-state [consumer-state-ref topic partition]
  (dosync (alter consumer-state-ref (fn [m] (merge-with (partial merge-with merge) m {topic {partition {:sent-request true}}})))))

(defn start-fetch-updater [consumer topic partition consumer-state-ref {:keys [fetch-update-freq] :or {fetch-update-freq 5000}}]
  "Sets a background loop that every n  milliseconds sends a consume offset, but only if the partition-info 
   extracted from consume-state-ref has (>= (:current-offset partition-info) (:fetch-water-mark partition-info))"
  ;(prn "start fetch updater: " partition)
  (fixdelay fetch-update-freq 
            (try
              (if-let [partition-info (-> @consumer-state-ref (get topic) (get partition))]
                    (if (and (>= (:current-offset partition-info) (:fetch-water-mark partition-info)) (not (true? (:sent-request partition-info))))
                      (do
                           (prn "Send fetch request " topic " " partition)
                           (flip-sent-state consumer-state-ref topic partition)
			                     (send-fetch consumer [[topic [{:partition partition :offset (inc (:fetch-water-mark partition-info)) }]]]))
                       )
		                
                (prn "No partition info for " topic " " partition))
                       
              (catch Exception e (do 
                                   (prn "Error reading " (-> @consumer-state-ref (get topic) (get partition)))
                                   (error e (str "Error reading " (-> @consumer-state-ref (get topic) (get partition)) )))))))
  
(defn set-offsets-state [consumer-state-ref topic partition offsets]
  (dosync 
    (alter consumer-state-ref (fn [m] 
		                                                   (if (-> m (get topic) (get partition))
		                                                    m
		                                                    (do
                                                           (prn "offsets " offsets)
                                                           (merge-with (partial merge-with merge)  m 
                                                                                                  {topic {partition {:current-offset (first offsets)
		                                                                                                :fetch-water-mark (first offsets) } } } )))))))

(defn topic-offset-consumer [host port consumer-state-ref conf]
	(let [c (client host port (merge  
	                                   ;;parameters that can be over written
				                             {:max-concurrent-writes 4000
				                             :reuse-client true 
	                                   :write-buff 100
	                                   }
	                                   ;merge conf
	                                   conf
	                                   ;parameters tha cannot be overwritten
	                                   {
				                             ;:channel-options [[ALLOCATOR (PooledByteBufAllocator. false)]]
				                             :handlers [
				                                                           offset-response-decoder
				                                                           default-encoder 
				                                                           
				                                                           ]}))]
	     (read-print-ch "error" (:error-ch c))
      
       ;on every response received update the consumer-state-ref, only if it does not exist in the map yet
	     (go 
        (while true
          (let [v (<! (:read-ch c))]
            ;type of data expected: ({:topic "ping", :partitions ({:partition 0, :error-code 0, :offsets (0)})})
                    (doseq [{:keys [topic partitions]} v]
		                  (doseq [{:keys [partition error-code offsets]} partitions]
		                    (if (= error-code 0)
                          (set-offsets-state consumer-state-ref topic partition offsets)
		                      (error "Partition offset fetch error " topic " " partition " returned error " error-code)
		                       ))))))
       
	     {:client c
        :conf conf}
	
	  ))

(defn flip-sent-state2 [consumer-state-ref topic partition]
  (dosync (alter consumer-state-ref (fn [m] (merge-with (partial merge-with merge) m {topic {partition {:sent-request false}}})))))

(defn set-water-mark [consumer-state-ref high-water-mark-offset topic partition messages]
  (dosync
						                     (if-let [fetch-water-mark (->> messages (sort-by :offset) last :offset)]
														           (do ;(prn "setting fetch-water-mark " fetch-water-mark)
                                           (alter consumer-state-ref (fn [m]
														                                         (merge-with (partial merge-with merge) m {topic {partition {:high-water-mark-offset high-water-mark-offset
			                                                                                                                :fetch-water-mark fetch-water-mark} } })))))))
(defn topic-consumer [host port consumer-state-ref message-ch conf]
   (let [c (client host port (merge  
                                   ;;parameters that can be over written
			                             {:max-concurrent-writes 4000
			                             :reuse-client true 
                                   :write-buff 100
                                   }
                                   ;merge conf
                                   conf
                                   ;parameters tha cannot be overwritten
                                   {
			                             ;:channel-options [[ALLOCATOR (PooledByteBufAllocator. false)]]
			                             :handlers [
			                                                           fetch-response-decoder
			                                                           default-encoder 
			                                                           
			                                                           ]}))]
     
     ;
     (read-print-ch "error" (:error-ch c))

     (go 
       (while true
		       (let [v (<! (:read-ch c))]
		         ; {:correlation-id 11, :topics (["ping" ({:partition 0, :error-code 0, :high-water-mark-offset 56, :messages [{:offset 1, :message-size 18, :message [{:crc 2143009708, :key #<byte[] [B@5022d801>, :bts #<byte[] [B@1f9352c1>}]} {:offset 2, :message-size 17, :message [{:crc 4056112429, :key #<byte[] [B@11e0d19>, :bts #<byte[] [B@5bede4e1>}]}
					       ;(prn "read response " v) 
                 (doseq [[topic partitions] (:topics v)]
					           (doseq [{:keys [partition error-code high-water-mark-offset messages]} partitions]
					             (if (= error-code 0)
						             (do
                           (set-water-mark consumer-state-ref high-water-mark-offset topic partition messages)
                            (doseq [{:keys [offset message]} messages]
                                
                                (if (coll? message)
                                  (doseq [message-item message]
                                       (do (>! message-ch (assoc message-item :topic topic :partition partition :offset offset))))
                                  (>! message-ch (assoc message :topic topic :partition partition :offset offset)))))
                   
					              (error "Fetch error " topic " " partition))
                        ;reset send flag flip-sent-state2 [consumer-state-ref topic partition]
                        (flip-sent-state2 consumer-state-ref topic partition)
                        )))))
			         
     ;(read-print-in c)
     {:client c
      :conf conf}

  ))

(defn mark-msg-processed [consumer {:keys [offset topic partition]}]
   (let [consumer-state-ref (-> consumer :state :consumer-state-ref)
         storage (:storage :consumer)]
     (dosync 
       (alter consumer-state-ref (fn [m]
                                   (merge-with (partial merge-with merge) m {topic {partition {:current-offset offset}}}))))
     (offset-storage/save-offset topic partition offset)))
     
     
     
         
(defn consumer [ bootstrap-brokers topics {:keys [message-buff] :or {message-buff 100} :as conf}]
  ;set metadata requests and wait
  ;request las positions
  ;request 
  (let [consumer-state-ref (ref {})
        brokers-metadata (ref {})
        offset-clients  (ref {})
        fetch-clients   (ref {})
        message-ch      (chan message-buff)
        state {:brokers-metadata brokers-metadata
               :conf conf
               :topics topics
               :consumer-state-ref consumer-state-ref
               :offset-clients offset-clients}]
    ;request metatada
    (track-broker-partitions bootstrap-brokers brokers-metadata 5000 conf)
    ;block till some data appears in the brokers-metadata
    (loop [i 20]
      (if (>= 0 (count @brokers-metadata))
        (if (> i 0)
            (do 
              (Thread/sleep 1000)
              (recur (dec i)))
            (throw (RuntimeException. "No metadata for brokers found")))))
    
     ;for each topic partition, we get the broker and create an offset consumer while also sending the offset request
     ;on response the offset consumer will update the consumer-state-ref
     (doseq [topic topics]
       (if-let [partitions (get @brokers-metadata topic)]
         (doseq [[i broker] (map-indexed (fn ([] 0) ([a b] [a b])) partitions)]
              (dosync (alter offset-clients apply-get-create
                                               broker 
                                               (fn [offset-client]
                                                           (send-offset-request offset-client [[topic [{:partition i}]]]))
                                               (fn [] 
                                                 ;topic-offset-consumer [host port conf]
                                                 (topic-offset-consumer (:host broker) (:port broker) consumer-state-ref conf)
                                                 )))
              (dosync (alter fetch-clients apply-get-create broker
                             (fn [fetch-client]
                               ;start-fetch-updater [consumer topic partition consume-state-ref {:keys [fetch-update-freq] :or {fetch-update-freq 500}}]
                               (start-fetch-updater fetch-client topic i consumer-state-ref conf))
                             (fn []
                               ;create fetch consumer topic-consumer [host port consumer-state-ref conf]
                               (topic-consumer (:host broker) (:port broker) consumer-state-ref message-ch conf)
                               )))
              
              )))
         
     {:state state :conf conf :message-ch message-ch}
    ))
    
    
 (defn read-msg [{:keys [message-ch]}]
   (<!! message-ch))
  
  


 
  
  