(ns
  ^{:doc "Public API for the Pool and Keyed Pools

           All pools return objects of type PoolObj to get the value use pool-obj-val
          "}
  kafka-clj.pool.api)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; Pool Object types

;;;; All pools must return objects of this type
;;;; gen-ts the timestamp of the the PoolObj was first created
;;;; ts is the timestamp when the PoolObj was created/or returned to the pool

;;;; v is untyped and is the actual type of the object pooled
(deftype PoolObj [^long gen-ts ^long ts v])

(defn pool-obj [v] ^PoolObj
  (let [ts (System/currentTimeMillis)]
    (PoolObj. ts ts v)))

(defn pool-obj-ts [^PoolObj v] ^long
  (.ts v))

(defn pool-obj-update-ts
  "Update the ts of the pool obj and return a new instance of PoolObj"
  [^PoolObj v ^long ts]
  (PoolObj. (.gen-ts v) ts (.v v)))

(defn pool-obj-gen-ts [^PoolObj v] ^long
  (.gen-ts v))

(defn pool-obj-val [^PoolObj v]
  (.v v))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; Protocols

(defprotocol IKeyedObjPool
  (-get-keyed-pool [this k] "Returns an instance of IObjPool")
  (-keyed-pool-available? [this k] "Return the number of objects available in the pool keyed by k")
  (-keyed-pool-close [this k])
  (-keyed-pool-close-all [this])
  (-keyed-pool-stats [this] "The all the stats for each IObjPool stored in IKeyedObjPool")
  (-keyed-pool-remove-ttl! [this time-limit-ms] "Remove any objects in all keyed pools that has been live longer than time-limit-ms")
  (-keyed-pool-remove-idle! [this time-limit-ms] "Remove any objects in all keyed pools that has been idle (ie. not polled and returned in more than time-limit-ms)"))

(defprotocol IObjPool
  (-poll [this timeout-ms] "Return a pooled object of type PoolObj or throw a timeout exception")
  (-return [this v] "Return a pooled PoolObj object to the pool")
  (-available? [this] "Return the number of objects available")
  (-close-all [this])
  (-pool-stats [this] "Return the stats for the given pool")
  (-remove-ttl! [this time-limit-ms] "Remove any objects in the pool that has been live longer than time-limit-ms")
  (-remove-idle! [this time-limit-ms] "Remove any objects in the pool that has been idle (ie. not polled and returned in more than time-limit-ms)"))

(defn pool-stats
  "Return a map of stats for the IKeyedObjPool"
  [keyed-pool]
  (-keyed-pool-stats keyed-pool))

(defn poll
  "Polls from a keyed pool, first we get the keyed poll, and then run poll on that pool"
  ([keyed-pool k]
   (poll keyed-pool k Long/MAX_VALUE))
  ([keyed-pool k timeout-ms]
   (-poll (-get-keyed-pool keyed-pool k) timeout-ms)))

(defn return
  "Return to a keyed pool, first we get the keyed pool, and then run return on that pool
   v must be of type PoolObj
   Important: The actual object return is not checked to have been polled from the pool,
    its the caller's reponsibility to manage poll return sequences correctly."
  ([keyed-pool k v]
   {:pre [(instance? PoolObj v)]}
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
