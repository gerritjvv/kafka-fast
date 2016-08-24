(ns
  ^{:doc "Object pool using Sempahore and Clojure's atom

          Use create-atom-obj-pool to create an atom object pool
          These functions as made to be used primarily from the kafka-clj.pool.keyed namespace
          and the kafka-clj.pool.api is used to work with the keyed pool"}

  kafka-clj.pool.atom
  (:require [kafka-clj.pool.api :refer :all]
            [kafka-clj.pool.util :refer :all]
            [clojure.tools.logging :refer [error]])
  (:import (java.util.concurrent Semaphore TimeUnit TimeoutException)
           (java.util.concurrent.atomic AtomicBoolean)
           (kafka_clj.pool.api PoolObj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; atom pool specific functions


;;; private record for atom specific pool and internals
;;; uses a semaphore to handle limit checking and locking, note
;;; we only lock on the limit checks, adding and removing objects are lock free
;;; (validate-f ctx v) is called after polling for a value
;;; (destroy-f ctx v) is called on invalid objects
;;; (create-f ctx v) is called to create an object

;;; semantics for queue
;;; queue is a triple [0] = the last object returned on poll and can be ignored, its used in the swap semantics to get the obj to be returned on poll
;;;                   [1] = the actual pooled live objects
;;;                   [2] = any objects that can be safely destroyed
;;;
;;;  The queue values can only be destroyed or returned after a swap has been done and should be taken from the result of a swap operation
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
    (AtomicBoolean. false)))

(defn a-destroy
  "Run (destroy-f ctx v) and will always release a permit on the semaphore even if destroy-f throws an exception
   This function does not check if v was attained from the pool, this is the responsibility of the calling program

   Note: destroy-f is only called if (pool-obj-val v) is non nil
  "
  [{:keys [destroy-f ^Semaphore sem ctx]} v]
  (when v
    (try
      (when (pool-obj-val v)
        (destroy-f ctx v))
      (finally
        (.release sem)))))

(defn atom-closed? [^AtomicBoolean closed]
  (.get closed))

(defn atom-get-and-set! [^AtomicBoolean closed]
  (.getAndSet closed true))


(defn a-pool-close-all [{:keys [queue closed-atom] :as pool}]
  ;;destroy all live objects in the queue and reset queue to []
  (when-not (atom-get-and-set! closed-atom)
    (swap! queue

           (fn [[x xs]]
             (when (realized? x)
               (call-safely a-destroy pool @x))
             (doseq [v xs]
               (when (realized? v)
                 (call-safely a-destroy pool @v)))
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


(defn a-poll
  "arg: AtomPool
   return a PoolObj"
  [{:keys [ctx queue create-f ^Semaphore sem closed-atom]} timeout-ms]
  (if (atom-closed? closed-atom)
    (throw (RuntimeException. "Pool is closed"))
    (if
      (.tryAcquire sem (long timeout-ms) TimeUnit/MILLISECONDS)

      ;;;;; we get the [head and tail] of the tail, then return [head tail nil] then nth 0 returns the head.
      ;;;;; if nil we create a new object, remember we've already had a try acquire on the semaphore so we have a valid permit.

      (->
        (swap! queue (fn [[_ [x & xs]]] [(a-poll-check-or-create ctx x create-f) xs nil]))
        (nth 0)
        deref)

      (throw (TimeoutException.)))))

(defn a-validate-safely
  "Call validate in a try catch, if any exception nil is returned"
  [{:keys [ctx validate-f]} v]
  (try
    (when (and v (pool-obj-val v))
      (validate-f ctx v))
    (catch Exception e (do
                         (error e e)
                         nil))))
(defn a-poll-valid
  "Poll for a valid object, invalid objects are destroyed, this process is repeated till a valid object is found
   or timeout-ms have expired

   Note: validate-f is only called if (pool-obj-val v) is non nil\n
   "
  [pool timeout-ms]
  (let [timeout-sleep-init (Math/min 100 (Math/abs (long (/ (long timeout-ms) 4))))]

    (loop [v (a-poll pool timeout-ms) ts (System/currentTimeMillis) timeout-sleep timeout-sleep-init]
      (if (a-validate-safely pool v)                          ;;test valid
        v
        (do                                                   ;;otherwise we destroy the object, and try another one
          (call-safely a-destroy pool v)
          (assert-timeout! timeout-ms ts)                     ;;if timeout throw exception
          ;;we have a valid object but no timeout, we need to sleep a bit, the sleep time increases by timeout-sleep-init till it reaches timeout-ms

          (Thread/sleep timeout-sleep)
          (recur (a-poll pool timeout-ms) ts (Math/max (long timeout-ms) (long (+ timeout-sleep timeout-sleep-init)))))))))

(defn a-return
  "We add v back into the pool, and do not change the head
   v must be an instance of PoolObj"
  [{:keys [queue ^Semaphore sem]} v]
  {:pre [v (instance? PoolObj v)]}
  ;;all returned objects must be transformed to (delay PoolObj) instances to track idle objects
  (swap! queue (fn [[x xs]] [x (vec (conj xs (delay (pool-obj-update-ts v (now)))))]))
  (.release sem))

(defn a-available?
  "Number of objects available in the atom pool"
  [{:keys [^Semaphore sem]}]
  (.availablePermits sem))

(defn a-waiting-threads
  "Returns an estimate of the number of threads waiting to acquire"
  [{:keys [^Semaphore sem]}]
  (.getQueueLength sem))

(defn a-pool-stats
  "Return a map {:available <available objs> :waiting-threads <threads waiting to acquire an obj>}"
  [pool]
  {:available (a-available? pool)
   :waiting-threads (a-waiting-threads pool)})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;; invalidate idle objects


(defn destroy-all-objs!
  "Get timedout objecs from any idle objects and call destroy on them, this is a NOOP if the pool is closed"
  [pool objs]
  (doseq [obj objs]
    (try
      (a-destroy pool @obj)
      (catch Exception e (error e e))))
  ;;must return nil to allow gc on destroyed objects
  nil)

(defn a-destroy-timeouts!
  "Destroy pool objects timeout"
  [{:keys [queue closed-atom] :as pool} should-destroy? timeout-ms]
  (when-not (atom-closed? closed-atom)
    (let [
          ;;; used to partition  objects into keep and destroy
          ;;; to destroy an object it must have been realized and idle for longer than timeout-ms
          partition-f (fn [obj]
                        (if (should-destroy? obj)
                          :destroy
                          :keep))
          ;;; takes the queue atom values [h queue-objs d]
          ;;; d contains the actions that will destroy objects
          swap-f (fn [[h objs _ :as v]]
                   (if (and (not-empty objs))
                     (let [{:keys [destroy keep]} (group-by partition-f objs)]
                       [h keep (delay (destroy-all-objs! pool destroy))])
                     v))]

      ;;derefenceing the last item from the swap-f triggers the destroy-all-objs! to be called on the finall
      ;;result of the atom swap! operation
      (when-let [destroy-actions (last (swap! queue swap-f))]
        @destroy-actions))))

(defn a-destroy-idle-timeouts!
  [pool timeout-ms]
  (let [should-destroy? (fn [obj] (pool-obj-idle-timeout? @obj timeout-ms))]

    (a-destroy-timeouts!
      pool
      should-destroy?
      timeout-ms)))

(defn a-destroy-ttl-timeouts!
  [pool timeout-ms]
  (let [should-destroy? (fn [obj] (pool-obj-ttl-timeout? @obj timeout-ms))]
    ;;; used to partition idle objects into keep and destroy
    ;;; to destroy an object it must have been realized and idle for longer than timeout-ms

    (a-destroy-timeouts!
      pool
      should-destroy?
      timeout-ms)))
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;; Records

(defrecord AtomObjPool [ctx]
  IObjPool
  (-poll [_ timeout-ms]
    (a-poll-valid ctx timeout-ms))
  (-return [_ v]
    (a-return ctx v))

  (-available? [_]
    (a-available? ctx))

  (-close-all [_]
    (a-pool-close-all ctx))

  (-pool-stats [_]
    (a-pool-stats ctx))

  (-remove-ttl! [_ time-limit-ms]
    (a-destroy-ttl-timeouts! ctx time-limit-ms))

  (-remove-idle! [_ time-limit-ms]
    (a-destroy-idle-timeouts! ctx time-limit-ms)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; Public functiosn

;;; (create-f ctx)
;;; (validate-f ctx v) v is a PoolObj
;;; (destroy-f ctx v)  v is a PoolObj
(defn create-atom-obj-pool [ctx create-f validate-f destroy-f]
  (->AtomObjPool (a-pool ctx create-f validate-f destroy-f)))