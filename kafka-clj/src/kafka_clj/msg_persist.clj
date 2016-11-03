(ns kafka-clj.msg-persist
  (:require [clojure.tools.logging :refer [error info]])
  (:import [org.mapdb DB DBMaker HTreeMap]
           [java.util.concurrent TimeUnit]
           [java.util Map Collection Iterator]))

(defn tmp-dir []
  (System/getProperty "java.io.tmpdir"))

(defn closed? [{{:keys [db]} :retry-cache}]
  (.isClosed ^DB db))

(defn close-retry-cache [_])

(defn _it_next [^Iterator it]
  (try
    (.next it)
    (catch Throwable _ nil)))

(defn _map-values-seq [^Iterator it]
  (try
    (when (.hasNext it)
      (lazy-seq
        (when-let [n (_it_next it)]
          (cons
            n
            (_map-values-seq it)))))
    (catch Exception _ '())))

(defn map-values [^Map m]
  (when m
    (try
      (let [^Collection vals' (.values m)]
        (_map-values-seq (.iterator vals')))
      (catch Exception _ '()))))

(defn- format-val [m-vals]
  "Fix bug in DBMap that all keys return null when used as keywords"
  (into {}
    (map (fn [k v]
           [(-> k name keyword) v])
         (keys m-vals)
         (vals m-vals))))

(defn retry-cache-seq [{:keys [retry-cache] :as connector}]
    "Returns a sequence of values with format {:topic topic :v v} v is the value that was sent to write-to-retry-cache,

     Ignore NPE in calling vals on retry-cache, this happens sometimes during shutdown,
     Can return null items"
  (when (and (:cache retry-cache) (not (closed? connector)))
    (map #(try
           (format-val %)
           (catch Exception e (when-not (closed? connector) ;;only rethrow npe if the connector is not closed
                                             (throw e))))
         (map-values (:cache retry-cache)))))

(defn delete-from-retry-cache [{:keys [retry-cache]} key-val]
  (let [^DB db (:db retry-cache)
         ^Map m (:cache retry-cache)]
    (.remove m key-val);remove key
    (.commit db)
    (.compact db)))

(defn write-to-retry-cache [{:keys [retry-cache]} topic v]
  ;here v can be a function, that when called should return the msg sent, or a sequence of messages
   (let [^Map map (:cache retry-cache)
         ^DB db (:db retry-cache)

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
                .compressionEnable
                .make)
       map (-> db (.getTreeMap "kafka-retry-cache"))]
      {:db db :file file :cache map}))

(defn create-retry-cache [{:keys [retry-cache-file] :or {retry-cache-file (str (tmp-dir) "/kafka-retry-cache")} :as conf}]
  (try
    (_create-retry-cache conf)
    (catch Exception e (do ;delete the file and recreate
                         (clojure.java.io/delete-file retry-cache-file)
                         (_create-retry-cache conf)))))
  
;(defn remove-sent-message [{:keys [send-cache]} topic partition corr-id]
;  (if-let [^HTreeMap cache (:cache send-cache)]
;	    (.remove cache (str corr-id ":" topic ":" partition))))

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
                                 send-cache-expire-after-access] :or {send-cache-size-limit 4
                                                                      send-cache-max-entries 2000000
                                                                      send-cache-expire-after-write 5
                                                                      send-cache-expire-after-access 5}}]
  "Returns {:db db :cache cache}
   db is the DBMaker newMemoryDirectDB result
   and cach is a HTreeMap cache"
   (let [^DB db (-> (DBMaker/newMemoryDirectDB)
                 (.sizeLimit (int send-cache-size-limit))     ;limit store size to 4GB
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

;(defn close-send-cache [{:keys [send-cache]}]
;  (try
;    (if send-cache
;		    (.close ^DB (:db send-cache)))
;    (catch Exception e (error e e))))
