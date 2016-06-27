(ns
  ^{:doc "Concurrent keyed pool implementation using ConcurrentHashMap and ConcurrentLinkedQueue"}
  kafka-clj.pool-impl
  (:use criterium.core)
  (:import (java.util.concurrent ExecutorService Executors TimeUnit Semaphore TimeoutException)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; Protocols

(defprotocol IKeyedObjPool
  (-get-keyed-pool [this k] "Returns an instance of IObjPool" )
  (-keyed-pool-available? [this k] "Return the number of objects available in the pool keyed by k"))

(defprotocol IObjPool
  (-poll [this timeout-ms] "Return a pooled object or throw a timeout exception")
  (-return [this v] "Return a pooled object to the pool")
  (-available? [this] "Return the number of objects available"))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; atom pool specific functions


;;; private record for atom specific pool and internals
;;; uses a semaphore to handle limit checking and locking, note
;;; we only lock on the limit checks, adding and removing objects are lock free
(defrecord AtomPool [ctx queue create-f validate-f destroy-f ^Semaphore sem])

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
    (Semaphore. (int (get ctx :pool-limit 100)))))

(defn a-poll-check-or-create
  "If x is nil we call create-f in a delay.
   returns a delayed object, this function runs inside swap! so needs to be idempotent"
  [ctx x create-f]
  (if x
    x
    (delay
      (create-f ctx))))


;;;; TODO test TIMEOUT,
;;;; TEST POLL RETURN SEM COUNT and also TEST THAT WE GET THE SAME OBJECT BACK WHEN POLL AGAIN
;;;; DO Multi threaded testing

(defn a-poll
  "arg: AtomPool"
  [{:keys [ctx queue create-f ^Semaphore sem]} timeout-ms]
  (if
    (.tryAcquire sem (long timeout-ms) TimeUnit/MILLISECONDS)
    (deref
      ;;;;; we get the head and tail of the tail, then return [head tail] then nth 0 returns the head.
      ;;;;; if nil we create a new object, remember we've already had a try acquire on the semaphore so we have a valid permit.
      (nth (swap! queue (fn [[_ [x & xs]]] [(a-poll-check-or-create ctx x create-f) xs])) 0))

    (throw (TimeoutException.))))

(defn a-return [{:keys [queue ^Semaphore sem]} v]
  (swap! queue (fn [[x xs]] [x (vec (conj xs v))]))
  (.release sem))

(defn a-available?
  "Number of objects available in the atom pool"
  [{:keys [^Semaphore sem]}]
  (.availablePermits sem))

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

(defn get-keyed-pool
  "Get a pool keyed on k, if the pool doesn't exist yet its created
   pool-create-f: called (pool-create-f ctx) should create an instance of IObjPool"
  ([ctx m-atom k pool-create-f]
   (if-let [v (get @m-atom k)]
     v
     (get (add-keyed-pool ctx m-atom k pool-create-f) k))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; Records for protocols and constructor functions

(defrecord AtomObjPool [ctx]
  IObjPool
  (-poll [_ timeout-ms]
    (a-poll ctx timeout-ms))
  (-return [_ v]
    (a-return ctx v))

  (-available? [_]
    (a-available? ctx)))

(defrecord AtomKeyedObjPool [ctx m-atom pool-create-f]
  IKeyedObjPool
  (-get-keyed-pool [_ k]
    (get-keyed-pool ctx m-atom k pool-create-f))

  (-keyed-pool-available? [_ k]
    (-available? (get-keyed-pool ctx m-atom k pool-create-f))))


(defn create-atom-obj-pool [ctx create-f validate-f destroy-f]
  (->AtomObjPool (a-pool ctx create-f validate-f destroy-f)))

(defn create-atom-keyed-obj-pool
  "create-f is called to create a pool when required and is called (create-f ctx)"
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
  (let [create-f   (fn [_] 1)
        validate-f identity
        destroy-f  identity
        pool (a-pool nil create-f validate-f destroy-f)]
    (test-threads 8 10000 (fn []
                            (a-return pool (a-poll pool))))))


(defn run-test-a []
  (with-progress-reporting (bench (test-a) :verbose)))