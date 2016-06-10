(ns kafka-clj.pool
  "Object pooling for tcp consumer code"
  (:import (org.apache.commons.pool2.impl GenericKeyedObjectPoolConfig DefaultPooledObject GenericKeyedObjectPool)
           (org.apache.commons.pool2 PooledObject KeyedPooledObjectFactory KeyedObjectPool)))

(comment                                                    ;usage

  (require '[kafka-clj.consumer.pool :as pool] :reload)
  (def o (pool/object-pool (pool/keyed-obj-factory (fn [& _] 1) (fn [& _] true) (fn [& _] nil) {}) {:max-total 10}))
  (def v (pool/borrow o :a))
  (pool/release o :a v))

(defprotocol IPool
  (borrow
    [pool k timeout-ms]
    "Throws NoSuchElementException if no connection can be returned or timedout")
  (release [pool k obj])
  (invalidate! [pool k obj]))

(extend-protocol IPool
  KeyedObjectPool
  (borrow [pool k timeout-ms] (.borrowObject ^GenericKeyedObjectPool pool k (long timeout-ms)))
  (release [pool k obj] (.returnObject ^KeyedObjectPool pool k obj))
  (invalidate! [pool k obj]
    (.invalidateObject ^KeyedObjectPool pool k obj)))

(defn- word->camlcase [word]
  (str (Character/toUpperCase (char (nth word 0))) (subs word 1 (count word))))

(defn- camel-case [k]
  (let [sp (clojure.string/split (name k) #"[_-]")]
    (apply str (map word->camlcase sp))))

(defn- call-fn
  [class method n-args]
  (let [o    (gensym)
        args (repeatedly n-args gensym)
        [class method] (map symbol [class method])]
    (eval
      `(fn [~o ~@args]
         (. ~(with-meta o {:tag class})
            (~method ~@args))))))

(defn- set-prop [obj [k v]]
 (try
   ((call-fn (.getName (.getClass ^Object obj)) (str "set" (camel-case k)) 1) obj v)
   (catch IllegalArgumentException _ nil))
  obj)

(defn- lift [^PooledObject obj] (.getObject obj))

(defn config-obj [conf]
  ;;reason to believe that the eviction thread is causing a deadlock
  (let [conf-obj (reduce set-prop (GenericKeyedObjectPoolConfig.) {:block-when-exhausted true
                                                                   ;:time-between-eviction-runs-millis 30000
                                                                   :test-while-idle false
                                                                   :test-on-return false
                                                                   :test-on-borrow true
                                                                   ;:min-evictable-idle-time-millis 30000
                                                                   :max-total-per-key (get conf :consumer-conn-max-total-per-key 40)
                                                                   :max-total (get conf :consumer-conn-max-total 40)
                                                                   })]
    (reduce set-prop conf-obj conf)))

(defn close! [^KeyedObjectPool pool]
  (.close pool))

(defn keyed-obj-factory
  "Creates a keyed object factory that calls
    create -> (create-f k conf)
    validate -> (validate-f v conf)
    destroy -> (destroy-f v conf)"
  [create-f validate-f destroy-f conf]
  (reify KeyedPooledObjectFactory
    (makeObject [_ k] (DefaultPooledObject. (create-f k conf)))
    (destroyObject [_ _ v]
      (when (and v (lift v))
        (destroy-f (lift v) conf)))
    (validateObject [_ _ v] (validate-f (lift v) conf))
    (activateObject [_ _ _])
    (passivateObject [_ _ _])))


(defn object-pool
  "Returns a KeyedObjectPool
  factory must be from keyed-obj-factory
  conf is map of settings from GenericKeyedObjectPoolConfig e.g setMaxTotal is {:max-total 10}"
  [^KeyedPooledObjectFactory factory conf]
  (GenericKeyedObjectPool. factory (config-obj conf)))

(defn pool-stats
  "Return a stats map for pools returned from the object-pool function"
  [^GenericKeyedObjectPool pool]
  {:num-active (.getNumActive pool)
   :num-idle (.getNumIdle pool)
   :num-waiters (.getNumWaiters pool)})