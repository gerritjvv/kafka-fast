(ns
  ^{:doc "
          Concurrent keyed pool implementation using Semaphore and Clojure's Atom
          The aim is to avoid explicit locking (apart from the semaphore) at all times.


          Idle objects and pool shrinking cannot be done due to the lack of lock usage and how atom's work<br/>
          if used we might get to the situation where a single object is closed by an idle check thread while being used.

          "}
  kafka-clj.pool-impl
  (:require [clojure.tools.logging :refer [error]])
  (:use criterium.core)
  (:import (java.util.concurrent ExecutorService Executors TimeUnit Semaphore TimeoutException ThreadFactory ScheduledExecutorService)))

;;@TODO CLOSE-ALL on keyed pool does not remove the idle check function :( we need to create an explicit test for this
;; 1 ensure all keyes are removed
;; 2 ensure that each pool's exec has been shutdown
;;@TODO write a test for the idle-check to ensure its working

(defonce DEFAULT-MIN-POOL-SIZE 1)
(defonce DEFAULT-IDLE-TIMEOUT-MS 60000)                     ;;1 minute

(declare idle-check!)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; Protocols

(defprotocol IKeyedObjPool
  (-get-keyed-pool [this k] "Returns an instance of IObjPool")
  (-keyed-pool-available? [this k] "Return the number of objects available in the pool keyed by k")
  (-keyed-pool-close [this k])
  (-keyed-pool-close-all [this]))

(defprotocol IObjPool
  (-poll [this timeout-ms] "Return a pooled object or throw a timeout exception")
  (-return [this v] "Return a pooled object to the pool")
  (-available? [this] "Return the number of objects available")
  (-close-all [this]))

(defn poll
  "Polls from a keyed pool, first we get the keyed poll, and then run poll on that pool"
  ([keyed-pool k]
   (poll keyed-pool k Long/MAX_VALUE))
  ([keyed-pool k timeout-ms]
   (-poll (-get-keyed-pool keyed-pool k) timeout-ms)))

(defn return
  "Return to a keyed pool, first we get the keyed pool, and then run return on that pool

   Important: The actual object return is not checked to have been polled from the pool,
    its the caller's reponsibility to manage poll return sequences correctly."
  ([keyed-pool k v]
   (-return (-get-keyed-pool keyed-pool k) v)))

(defn avaiable?
  "Return the available objects in the pool keyed by k"
  [keyed-pool k]
  (-keyed-pool-available? keyed-pool k))

(defn close-all
  ([keyed-pool]
   (-keyed-pool-close-all keyed-pool))
  ([keyed-pool k]
   (-keyed-pool-close keyed-pool k)))

;;used to store and object in the pool and determine its idle time
;;ts means the time when the obj was added to the pool
(deftype PoolObj [^long ts v])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; util functions

(defn call-safely [f & args]
  (try
    (apply f args)
    (catch Exception e (error e e))))

(defn timeout? [timeout-ms start-ts]
  (> (- (System/currentTimeMillis) (long start-ts)) (long timeout-ms)))

(defn assert-timeout!
  "When timeout-ms have passed since start-ts a TimeoutException is thrown"
  [timeout-ms start-ts]
  (when (timeout? timeout-ms start-ts)
    (throw (TimeoutException.))))

(defn ^PoolObj create-pool-obj [v]
  (PoolObj. (System/currentTimeMillis) v))

(defn pool-obj-val [^PoolObj v]
  (.v v))

(defn pool-obj-idle-timeout?
  "True if (diff curr-time (.ts v)) > timeout-ms"
  [^PoolObj v timeout-ms]
  (timeout? timeout-ms (.ts v)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; atom pool specific functions


;;; private record for atom specific pool and internals
;;; uses a semaphore to handle limit checking and locking, note
;;; we only lock on the limit checks, adding and removing objects are lock free
;;; (validate-f ctx v) is called after polling for a value
;;; (destroy-f ctx v) is called on invalid objects
;;; (create-f ctx v) is called to create an object
(defrecord AtomPool [ctx queue create-f validate-f destroy-f ^Semaphore sem closed-atom])

(defn a-pool
  "create-f when called should create a valid object called as (create-f ctx),
   validate-f is called before poll returns, called (validate-f ctx v)
   destroy-f  is called to destroy an object (destroy-f ctx v)
   sem is a semaphore setting up the limit = (get ctx :pool-limit 100)"
  [ctx create-f validate-f destroy-f]
  {:pre [(every? fn? [create-f validate-f destroy-f])]}
  (->AtomPool
    ctx
    (atom [nil []])
    create-f
    validate-f
    destroy-f
    (Semaphore. (int (get ctx :pool-limit 100)))
    (atom false)))

(defn a-destroy
  "Run (destroy-f ctx v) and will always release a permit on the semaphore even if destroy-f throws an exception
   This function does not check if v was attained from the pool, this is the responsibility of the calling program
  "
  [{:keys [destroy-f ^Semaphore sem ctx]} v]
  (when v
    (try
      (destroy-f ctx v)
      (finally
        (.release sem)))))

(defn a-pool-close-all [{:keys [queue closed-atom] :as pool}]
  ;;destroy all live objects in the queue and reset queue to []
  (when-not @closed-atom
    (swap! queue

           (fn [[x xs]]
             (call-safely a-destroy pool (pool-obj-val x))
             (doseq [v xs]
               (call-safely a-destroy pool (pool-obj-val v)))
             []))))

(defn a-poll-check-or-create
  "If x is nil we call create-f in a delay.
   returns a delayed object, this function runs inside swap! so needs to be idempotent"
  [ctx x create-f]
  (if x
    x
    (delay
      ;; we must add all objects as PoolObj instances to track idle time
      (create-pool-obj (create-f ctx)))))

(defn debug-val [v]
  (spit "/tmp/debug-val.txt" (str "debug-val " v) :append true)
  v)

(defn a-poll
  "arg: AtomPool"
  [{:keys [ctx queue create-f ^Semaphore sem closed-atom]} timeout-ms]
  (if @closed-atom
    (throw (RuntimeException. "Pool is closed"))
    (if
      (.tryAcquire sem (long timeout-ms) TimeUnit/MILLISECONDS)

      ;;;;; we get the [head and tail] of the tail, then return [head tail] then nth 0 returns the head.
      ;;;;; if nil we create a new object, remember we've already had a try acquire on the semaphore so we have a valid permit.

      (->
        (swap! queue (fn [[_ [x & xs]]] [(a-poll-check-or-create ctx x create-f) xs]))
        (nth 0)
        deref
        pool-obj-val)

      (throw (TimeoutException.)))))

(defn a-validate-safely
  "Call validate in a try catch, if any exception nil is returned"
  [{:keys [ctx validate-f]} v]
  (try
    (validate-f ctx v)
    (catch Exception e (do
                         (error e e)
                         nil))))
(defn a-poll-valid
  "Poll for a valid object, invalid objects are destroyed, this process is repeated till a valid object is found
   or timeout-ms have expired
   "
  [pool timeout-ms]
  (loop [v (a-poll pool timeout-ms) ts (System/currentTimeMillis)]
    (if (a-validate-safely pool v)                          ;;test valid
      v
      (do                                                   ;;otherwise we destroy the object, and try another one
        (call-safely a-destroy pool v)
        (assert-timeout! timeout-ms ts)                     ;;if timeout throw exception
        (recur (a-poll pool timeout-ms) ts)))))

(defn a-return
  "We add v back into the pool, and do not change the head"
  [{:keys [queue ^Semaphore sem]} v]
  ;;all returned objects must be transformed to PoolObj instances to track idle objects
  (swap! queue (fn [[x xs]] [x (vec (conj xs (delay (create-pool-obj v))))]))
  (.release sem))

(defn a-available?
  "Number of objects available in the atom pool"
  [{:keys [^Semaphore sem]}]
  (.availablePermits sem))

(comment
  (defn idle-check!
    "Remove and destroy any objects that have been in idle for too long"
    [{:keys [queue ctx] :as pool}]
    {:pre [queue]}
    (prn "run idle check")
    (swap! queue
           (fn [[x xs]]
             (let [{:keys [timeout valid]} (group-by #(if (pool-obj-idle-timeout? % (get ctx :idle-timeout-ms DEFAULT-IDLE-TIMEOUT-MS)) :timeout :valid) xs)

                   ;; we should only destroy as many objects as to leave alive min-pool-size
                   destroy-count (- (count xs) (get ctx :min-pool-size DEFAULT-MIN-POOL-SIZE))
                   timeout-to-live (drop destroy-count timeout)
                   timeout-to-destroy (take destroy-count timeout)]

               ;;destroy ignoring exceptions all objects in timeout-to-destroy
               (doseq [v timeout-to-destroy]
                 (call-safely a-destroy pool (pool-obj-val v)))

               ;;return the valid and timeout-to-live merged
               [x (apply conj (vec valid) timeout-to-live)])))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; keyed pool specific functions

(defn add-keyed-pool
  "
   Called from get-keyed-pool
   Add a pool keyed on k to m-atom
   pool-create-f: called (pool-create-f ctx) should create an instance of IObjPool"
  [ctx m-atom k pool-create-f]
  (swap! m-atom (fn [m]
                  (if (get m k)
                    m
                    (assoc m k (pool-create-f ctx))))))

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



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; Records for protocols and constructor functions

(defn identity-f
  "Return a identity function that take any amount of arguments and always return v"
  [v]
  (fn [& _] v))

(defrecord AtomObjPool [ctx]
  IObjPool
  (-poll [_ timeout-ms]
    (a-poll-valid ctx timeout-ms))
  (-return [_ v]
    (a-return ctx v))

  (-available? [_]
    (a-available? ctx))

  (-close-all [_]
    (a-pool-close-all ctx)))

(defrecord AtomKeyedObjPool [ctx m-atom pool-create-f]
  IKeyedObjPool
  (-get-keyed-pool [_ k]
    (get-keyed-pool ctx m-atom k pool-create-f))

  (-keyed-pool-available? [_ k]
    (-available? (get-keyed-pool ctx m-atom k pool-create-f)))

  (-keyed-pool-close-all [_]
    (remove-keyed-pool-all m-atom))

  (-keyed-pool-close [_ k]
    (remove-keyed-pool m-atom k)))


(defn create-atom-obj-pool [ctx create-f validate-f destroy-f]
  (->AtomObjPool (a-pool ctx create-f validate-f destroy-f)))

(defn create-atom-keyed-obj-pool
  "
   ctx = :pool-limit int max objects each pool keyed can have
   value-create-f is called to create a pool when required and is called (create-f ctx)
   value-validate-f is called before returning a polled object, the validation is done outside the polling function such that it doesn't block other threads from polling
                    this allows possibly blocking validtion not to hurt performance for other threads, called using (value-validate-f ctx v)
   value-destroy-f  invalid objects are destroyed and removed from the pool called using (value-destroy-f ctx v)
  "
  ([ctx value-create-f value-validate-f value-destroy-f]
   (create-atom-keyed-obj-pool ctx #(create-atom-obj-pool % value-create-f value-validate-f value-destroy-f)))
  ([ctx pool-create-f]
   (->AtomKeyedObjPool ctx (atom {}) pool-create-f)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; test and benchmark functions

(defn test-threads [n i f]
  (let [^ExecutorService service (Executors/newFixedThreadPool n)
        f2 (fn []
             (try
               (dotimes [_ i]
                 f)
               (catch Exception e (.printStackTrace e))))]

    (dotimes [_ n]
      (.submit service ^Runnable f2))

    (.shutdown service)
    (.awaitTermination service 10 TimeUnit/MINUTES)))



(defn test-a []
  (let [create-f (fn [_] 1)
        validate-f identity
        destroy-f identity
        pool (a-pool nil create-f validate-f destroy-f)]
    (test-threads 8 10000 (fn []
                            (a-return pool (a-poll pool 60000))))))


(defn run-test-a []
  (with-progress-reporting (bench (test-a) :verbose)))