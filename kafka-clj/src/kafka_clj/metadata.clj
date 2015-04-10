(ns kafka-clj.metadata
  (:require 
            [clj-tuple :refer [tuple]]
            [kafka-clj.produce :refer [metadata-request-producer send-metadata-request shutdown]]
            [fun-utils.core :refer [fixdelay]]
            [clojure.tools.logging :refer [info error warn]]
            [clojure.core.async :refer [go <! <!! >!! alts!! timeout thread]])
  (:import [java.nio ByteBuffer]
           [clj_tcp.client Poison Reconnected]
           (java.util.concurrent.atomic AtomicBoolean)))

"Keeps track of the metadata
 "

(defn convert-metadata-response [resp]
  ;; transform the resp into a map
  ;; {topic-name [{:host host :port port :isr [ {:host :port}] :error-code code} ] }
  ;; the index of the vector (value of the topic-name) is sorted by partition number 
  ;; here topic-name:String and partition-n:Integer are keys but not keywords
  ;;{:correlation-id 2,
	;;											 :brokers [{:node-id 0, :host a, :port 9092}],
	;;											 :topics
	;;											 [{:error-code 10,
	;;											   :topic p,
	;;											   :partitions
	;;											   [{:partition-error-code 10,
	;;											     :partition-id 0,
	;;											     :leader 0,
	;;											     :replicas '(0 1),
	;;											     :isr '(0)}]}]}"
  (let [m (let [;convert the brokers to a map {:broker-node-id {:host host :port port}}
                brokers-by-node (into {} (map (fn [{:keys [node-id host port]}] [ node-id {:host host :port port}]) (:brokers resp)))]
                ;convert the response message to a map {topic-name {partition {:host host :port port}}}
                (into {} 
			                 (for [topic (:topics resp) :when (= (:error-code topic) 0)]
			                      [(:topic topic) (apply tuple (vals (apply sorted-map (flatten
																																	                  (for [partition (:partitions topic)
																																	                         :let [broker (get brokers-by-node (:leader partition))
                                                                                                 isr (mapv #(get brokers-by-node %) (:isr partition))]]
																																	                     [(:partition-id partition)
                                                                                        {:host (:host broker)
                                                                                         :port (:port broker)
                                                                                         :isr  isr
                                                                                         :id (:partition-id partition)
                                                                                         :error-code (:partition-error-code partition)}])))))])))]
    m))

(defn send-update-metadata [producer conf]
  (send-metadata-request producer conf))

(defn get-broker-metadata [metadata-producer {:keys [metadata-timeout] :or {metadata-timeout 5000} :as conf}]
   "
   Creates a metadata-request-producer, sends a metadata request to the broker and waits for a result,
   if no result in $metadata-timeout or an error an exception is thrown, otherwise the result of
   (convert-metadata-response resp) is returned.
   "
   (let [producer metadata-producer
         read-ch  (-> producer :client :read-ch)
         error-ch (-> producer :client :error-ch)]
	      (send-update-metadata producer conf)
	          ;wait for response or timeout
	          (let [[v c] (alts!! [read-ch error-ch (timeout metadata-timeout)])]
	             (if v
	               (if (= c read-ch)  (convert-metadata-response v)
	                 (throw (Exception. (str "Error reading metadata from producer " metadata-producer  " error " v))))
	               (do
                   (shutdown producer)
                   (throw (Exception. (str "timeout reading from producer " (vals metadata-producer)))))))))

(defn- is-blacklisted?
  [{:keys [host port] :as producer} blacklisted-producers]
  (get blacklisted-producers (str host ":" port)))

(defn smart-deref [x]
  (if (instance? clojure.lang.IDeref x) (deref x) x))

(defn- black-list-producer [blacklisted-metadata-producers-ref {:keys [host port]} e]
  (warn (str "Blacklisting metadata-producer: " host ":" port) e)
  (dosync (commute blacklisted-metadata-producers-ref assoc (str host ":" port) true))
  nil)

(defn blacklist-if-exception [blacklisted-metadata-producers-ref metadata-producer f & args]
  ;(info "Metadata producer1: " (:host metadata-producer) ":" (:port metadata-producer) ": is closed " (get-in metadata-producer [:client :closed]))
  (try
    (apply f args)
    (catch Exception e [metadata-producer (black-list-producer blacklisted-metadata-producers-ref metadata-producer e)])))

(defn _get-metadata [metadata-producer conf blacklisted-metadata-producers-ref]
  (blacklist-if-exception blacklisted-metadata-producers-ref metadata-producer (fn [] (let [meta (get-broker-metadata metadata-producer conf)]
                                                                                        [metadata-producer (if (empty? meta) nil meta)]))))

(defn iterate-metadata-producers [metadata-producers conf blacklisted-metadata-producers-ref]
  ;(prn "meta : " metadata-producers)
  (->>
    metadata-producers
    smart-deref
    (map smart-deref)
    (filter (complement nil?))
    (filter (complement #(is-blacklisted? % @blacklisted-metadata-producers-ref)))
    (map #(_get-metadata % conf blacklisted-metadata-producers-ref))
    (filter (fn [[_ meta]] (not (nil? meta))))
    first))

(defn get-metadata [metadata-producers conf & {:keys [blacklisted-metadata-producers-ref] :or {blacklisted-metadata-producers-ref (ref {})}}]
  (let [[metadata-producer meta] (iterate-metadata-producers metadata-producers conf blacklisted-metadata-producers-ref)]
    ;(info "Got meta from " (:host metadata-producer) " -> " meta)
    meta))

(defn- client-closed? [producer]
  (.get ^AtomicBoolean (get-in producer [:client :closed])))

(defn- recreate-producer-if-closed! [conf metadata-producer]
  (if (client-closed? metadata-producer)
    (metadata-request-producer (:host metadata-producer) (:port metadata-producer) conf)
    metadata-producer))

(defn get-metadata-recreate! [metadata-producers conf & args]
  (try
    [metadata-producers (apply get-metadata (smart-deref metadata-producers) conf args)]
    (catch Exception e (let [producers2 (doall (map (partial recreate-producer-if-closed! conf) metadata-producers))]
                         [producers2 (apply get-metadata producers2 conf args)]))))
