(ns kafka-clj.offset-storage)

"Offset storage management"


(defn create-mysql-storage [conf]

   
  )


(defn create-in-memory-storage [conf]
  (let [m (ref {})]
    
  
  
  ))

(defn lock [topic] true)
(defn unlock [topic] )

(defn get-offset [topic]
  0)

(defn save-offset [topic partition offset]
  )

(defn get-storage-connector [{:keys [storage-type] :or {storage-type 0} :as conf}]
  (cond (= storage-type 0) (create-in-memory-storage conf)
    (= storage-type 1) (create-mysql-storage)
    :else (throw (RuntimeException. (str "Storage type " storage-type " is not supported")))))

