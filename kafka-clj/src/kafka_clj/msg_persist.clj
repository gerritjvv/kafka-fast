(ns kafka-clj.msg-persist
  (:require [clojure.tools.logging :refer [error info]])
  (:import [org.mapdb DB DBMaker HTreeMap]
           [java.util.concurrent TimeUnit]
           [java.util Map]))

(defn close-retry-cache [{:keys [retry-cache]}]
  (try (do
         ;running this code causes 100% cpu usages,
         ;for now we use close on JVM shutdown
         ;(.close (:cache retry-cache))
         )
       
       (catch Exception e (error e e))))

(defn- format-val [m-vals]
  "Fix bug in DBMap that all keys return null when used as keywords"
  (into {}
    (map (fn [k v]
           [(-> k name keyword) v])
         (keys m-vals)
         (vals m-vals))))
      
        
(defn retry-cache-seq [{:keys [retry-cache]}]
    "Returns a sequence of values with format {:topic topic :v v} v is the value that was sent to write-to-retry-cache"
    (map format-val (vals (:cache retry-cache))))

(defn delete-from-retry-cache [{:keys [retry-cache]} key-val]
  (let [^DB db (:db retry-cache)
         ^Map m (:cache retry-cache)]
    ;delete whole map
    (.remove m key-val);remove key
    (.commit db)
    (.compact db)))
    
    
    

(defn write-to-retry-cache [{:keys [retry-cache]} topic v]
  ;here v can be a function, that when called should return the msg sent, or a sequence of messages
   (let [^Map map (:cache retry-cache)
         ^DB db (:db retry-cache)
         msgs (if (fn? v) (v nil)  v)
         msg-val {:topic topic :v v}
         key-val (hash msg-val)]
     (.put map key-val (assoc msg-val :key-val key-val))
     (.commit db)))


(defn _create-retry-cache [{:keys [retry-cache-file retry-cache-delete-on-exit] :or {retry-cache-delete-on-exit false retry-cache-file "/tmp/kafka-retry-cache"}}]
 (let [file (clojure.java.io/file  retry-cache-file)
       ^DBMaker dbmaker (DBMaker/newFileDB file)
       _ (if retry-cache-delete-on-exit (.deleteFilesAfterClose dbmaker)) ;for testing
       db (->   dbmaker 
                .closeOnJvmShutdown
                .cacheDisable
                .compressionEnable                .make)
       map (-> db (.getTreeMap "kafka-retry-cache"))]
      {:db db :file file :cache map}))

(defn create-retry-cache [{:keys [retry-cache-file] :or {retry-cache-file "/tmp/kafka-retry-cache"} :as conf}]
  (try
    (_create-retry-cache conf)
    (catch Exception e (do ;delete the file and recreate
                         (clojure.java.io/delete-file retry-cache-file)
                         (_create-retry-cache conf)))))
  
(defn remove-sent-message [{:keys [send-cache]} topic partition corr-id] 
  (if-let [^HTreeMap cache (:cache send-cache)]
	    (.remove cache (str corr-id ":" topic ":" partition))))

(defn cache-sent-messages
  "Offsets is expected to have format [[corr-id msgs]...]
   msgs is a list of messages (of maps with keys topic partition), and corr-id a long value
   For any msgs sequence all messages must be from the same partition."
   [{:keys [send-cache] } offsets]
  (if-let [^Map cache (:cache send-cache)]
    (doseq [[corr-id msgs] offsets]
	    (let [{:keys [topic partition]} (first msgs)]
	       (.put cache (str corr-id ":" topic ":" partition) msgs)))))

(defn get-sent-message [{:keys [send-cache]} topic partition corr-id]
  "Return the messages from the cache if any, the key is also removed from the cache."
  (let [^HTreeMap cache (:cache send-cache)
        k (str corr-id ":" topic ":" partition)
        msgs (.get cache k)]
     (if msgs (.remove cache corr-id))
    msgs))

(defn create-send-cache [{:keys [send-cache-size-limit
                                 send-cache-max-entries
                                 send-cache-expire-after-write
                                 send-cache-expire-after-access] :or {send-cache-size-limit 2
                                                                      send-cache-max-entries 1000000
                                                                      send-cache-expire-after-write 5 
                                                                      send-cache-expire-after-access 5}}]
  "Returns {:db db :cache cache}
   db is the DBMaker newMemoryDirectDB result
   and cach is a HTreeMap cache"
   (let [^DB db (-> (DBMaker/newMemoryDirectDB)
                 (.sizeLimit (int send-cache-size-limit))     ;limit store size to 2GB
                 (.transactionDisable)    ;better performance
                 (.make))
         
         ^HTreeMap cache  (-> db
				                (.createHashMap "kafka-send-cache")
				                (.expireMaxSize send-cache-max-entries)
				                (.expireAfterWrite send-cache-expire-after-write TimeUnit/SECONDS)
				                (.expireAfterAccess send-cache-expire-after-access  TimeUnit/SECONDS)
				                (.make))
         
         ]
     
     {:db db :cache cache}))

(defn close-send-cache [{:keys [send-cache]}]
  (try 
    (if send-cache
		    (.close ^DB (:db send-cache)))
    (catch Exception e (error e e))))
