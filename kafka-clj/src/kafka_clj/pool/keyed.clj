(ns
  ^{:doc "Functions and records for a KeyedPool.
          A keyed pool contains one IObjPool instance per key
             usage: tcp connection pool per server where the key is the server host and port

          Use the create-atom-keyed-obj-pool function to create the pool
          then use the kafka-clj.pool.api functions to poll, return and close the pool"}
  kafka-clj.pool.keyed
  (:require [kafka-clj.pool.api :refer :all]
            [kafka-clj.pool.atom :refer :all]
            [kafka-clj.pool.util :refer :all]
            [clojure.tools.logging :refer [error]])
  (:import (java.util.concurrent ScheduledExecutorService Executors ThreadFactory TimeUnit)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; keyed pool specific functions

(defn add-keyed-pool
  "
   Called from get-keyed-pool
   Add a pool keyed on k to m-atom
   pool-create-f: called (pool-create-f ctx k) should create an instance of IObjPool"
  [ctx m-atom k pool-create-f]
  (swap! m-atom (fn [m]
                  (if (get m k)
                    m
                    (assoc m k (pool-create-f ctx k))))))

(defn remove-keyed-pool [m-atom k]
  (swap! m-atom (fn [m]
                  (if-let [p (get m k)]
                    (do
                      (call-safely -close-all p)
                      (dissoc! m k))
                    m))))

(defn remove-keyed-pool-all [m-atom]
  (swap! m-atom
         (fn [m]
           (reduce-kv (fn [_ k v] (call-safely -close-all v)) nil m)
           {})))

(defn get-keyed-pool
  "Get a pool keyed on k, if the pool doesn't exist yet its created
   pool-create-f: called (pool-create-f ctx) should create an instance of IObjPool"
  ([ctx m-atom k pool-create-f]
   (if-let [v (get @m-atom k)]
     v
     (get (add-keyed-pool ctx m-atom k pool-create-f) k))))

(defn join-key [k]
  (if (coll? k)
    (clojure.string/join "/" k)
    (str k)))

(defn keyed-pool-stats [m-atom]
  (let [r-f (fn [state k apool]
              (assoc state (join-key k) (-pool-stats apool)))]
    (reduce-kv r-f {} @m-atom)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; Records for protocols and constructor functions

(defn identity-f
  "Return a identity function that take any amount of arguments and always return v"
  [v]
  (fn [& _] v))

;;;; A managed keyed pool
;;;;; managing idle pool objects, and delegates all operations to the keyed-pool
;;;;; ttl is the ttl for each connection, this allows connections to be expired after being open for a period of time,
;;;;;  good practice with long open connections given that OSes have the tendency to close them after a while any how and
;;;;;  leave the application with broken pipe errors

(defrecord ManagedObjPool [keyed-pool ^ScheduledExecutorService exec-service ttl-ms]
  IKeyedObjPool
  (-get-keyed-pool [_ k]
    (-get-keyed-pool keyed-pool k))

  (-keyed-pool-available? [_ k]
    (-keyed-pool-available? keyed-pool k))

  (-keyed-pool-close-all [_]
    (-keyed-pool-close-all keyed-pool))

  (-keyed-pool-close [_ k]
    (close-scheduled-exec-service! exec-service)
    (-keyed-pool-close keyed-pool k))

  (-keyed-pool-stats [_]
    (-keyed-pool-stats keyed-pool))

  (-keyed-pool-remove-idle! [_ time-limit-ms]
    (-keyed-pool-remove-idle! keyed-pool time-limit-ms))

  (-keyed-pool-remove-ttl! [_ time-limit-ms]
    (-keyed-pool-remove-ttl! keyed-pool time-limit-ms)))


;;; m-atom is a map of keys and values as IObjPool instances
;;; the pool-create-f is called (pool-create-f ctx k) where k is the key passed in.

(defrecord AtomKeyedObjPool [ctx m-atom pool-create-f]
  IKeyedObjPool
  (-get-keyed-pool [_ k]
    (get-keyed-pool ctx m-atom k pool-create-f))

  (-keyed-pool-available? [_ k]
    (-available? (get-keyed-pool ctx m-atom k pool-create-f)))

  (-keyed-pool-close-all [_]
    (remove-keyed-pool-all m-atom))

  (-keyed-pool-close [_ k]
    (remove-keyed-pool m-atom k))

  (-keyed-pool-stats [_]
    (keyed-pool-stats m-atom))

  (-keyed-pool-remove-ttl! [_ time-limit-ms]
    (doseq [[_ pool] @m-atom]
      (-remove-ttl! pool time-limit-ms)))

  (-keyed-pool-remove-idle! [_ time-limit-ms]
    (doseq [[_ pool] @m-atom]
      (-remove-idle! pool time-limit-ms))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; Public functions

;;;;;;; To poll, return etc please use the functions in the kafka-clj.pool.api namespace

(defn create-keyed-obj-pool
  "
   ctx = :pool-limit int max objects each pool keyed can have

   value-create-f is called to create a pool when required and is called (create-f ctx k) where k is the key used on the pool
   value-validate-f is called before returning a polled object, the validation is done outside the polling function such that it doesn't block other threads from polling
                    this allows possibly blocking validtion not to hurt performance for other threads, called using (value-validate-f ctx k v)
   value-destroy-f  invalid objects are destroyed and removed from the pool called using (value-destroy-f ctx k v)

   pool-create-f will be called as (pool-create-f ctx k) where k is the key for the pool
  "
  ([ctx value-create-f value-validate-f value-destroy-f]
    ;;note that here use change the value-*f functions to pass in the k (key) parameter which the atom pool does not

   (create-keyed-obj-pool ctx (fn [ctx k] (create-atom-obj-pool ctx
                                                                (fn [ctx]
                                                                       (value-create-f ctx k))
                                                                (fn [ctx v] (value-validate-f ctx k v))
                                                                (fn [ctx v] (value-destroy-f ctx k v))))))
  ([ctx pool-create-f]
   (->AtomKeyedObjPool ctx (atom {}) pool-create-f)))


(defn create-managed-keyed-obj-pool
  "ctx = :idle-limit-ms  the time an object can be idle till its removed permemantly from the pool 60 seconds
         :idle-check-freq-ms time to check for idle timeouts 60 seconds
         :ttl-ms time to live ms that a connection closed afterwards no matter if its being used or not, default 15 minutes
   "
  [ctx keyed-obj-pool]
  (let [exec-service (scheduled-exec-service "keyed-pool-idle-check-service") ;;closed in the ManagedObjPool:-keyed-pool-close-all

        ttl-ms (get ctx :ttl-ms 900000)
        managed-pool (->ManagedObjPool keyed-obj-pool keyed-obj-pool ttl-ms)
        idle-limit-ms (get ctx :idle-limit-ms 60000)

        check-idle-f #(try
                       (-keyed-pool-remove-ttl! managed-pool ttl-ms)
                       (-keyed-pool-remove-idle! managed-pool idle-limit-ms)
                       (catch Exception e (error e e)))]

    (schedule exec-service
              (get ctx :idle-check-freq-ms 60000)
              check-idle-f)

    managed-pool))