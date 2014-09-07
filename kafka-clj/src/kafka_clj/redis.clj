(ns
    kafka-clj.redis
  (:import
    (org.apache.commons.pool2 ObjectPool PooledObjectFactory)
    (org.apache.commons.pool2.impl GenericObjectPool DefaultPooledObject GenericKeyedObjectPool))
    (:require [taoensso.carmine
               (protocol    :as protocol)
               (connections :as conns)
               (commands    :as commands)]
              [taoensso.carmine :as car]
              [taoensso.timbre  :as timbre]
              [clojure.tools.logging :refer [info]]))



(defn- make-connection-factory [{:keys [host port timeout] :or {host "localhost" port 6379 timeout 300}}]
  (reify PooledObjectFactory
    (makeObject      [_ ] (DefaultPooledObject. (conns/make-new-connection {:host host :port port :timeout timeout})))
    (activateObject  [_ pooled-obj])
    (validateObject  [_ pooled-obj] (let [conn (.getObject pooled-obj)]
                                           (conns/conn-alive? conn)))
    (passivateObject [_ pooled-obj])
    (destroyObject   [_ pooled-obj] (let [conn (.getObject pooled-obj)]
                                           (conns/close-conn conn)))))

(defn- set-pool-option [^ObjectPool pool [opt v]]
  (case opt

    ;;; org.apache.commons.pool2.impl.BaseGenericObjectPool
    :block-when-exhausted? (.setBlockWhenExhausted pool v) ; true
    :lifo?       (.setLifo          pool v) ; true
    :max-active   (.setMaxTotal      pool v) ; -1
    :max-total   (.setMaxTotal      pool v) ; -1

    :﻿max-total-per-key (.setMaxTotal pool v)
    :max-wait-ms (.setMaxWaitMillis pool v) ; -1
    :min-evictable-idle-time-ms (.setMinEvictableIdleTimeMillis pool v) ; 1800000
    :num-tests-per-eviction-run (.setNumTestsPerEvictionRun     pool v) ; 3
    :soft-min-evictable-idle-time-ms (.setSoftMinEvictableIdleTimeMillis pool v) ; -1
    :swallowed-exception-listener    (.setSwallowedExceptionListener     pool v)
    :test-on-borrow?  (.setTestOnBorrow  pool v) ; false
    :test-on-return?  (.setTestOnReturn  pool v) ; false
    :test-while-idle? (.setTestWhileIdle pool v) ; false
    :time-between-eviction-runs-ms (.setTimeBetweenEvictionRunsMillis pool v) ; -1

    (prn "Unknown pool option: " opt))
  pool)


(defn conn-pool ^java.io.Closeable [spec pool-opts]
  (let [jedis-defaults ; Ref. http://goo.gl/y1mDbE
        {:test-while-idle?              true  ; from false
         :num-tests-per-eviction-run    -1    ; from 3
         :min-evictable-idle-time-ms    60000 ; from 1800000
         :time-between-eviction-runs-ms 30000 ; from -1
         }
        carmine-defaults
        {
          :max-total 40
         }
        opts (merge jedis-defaults carmine-defaults pool-opts)
        ]
    (conns/->ConnectionPool
      (reduce set-pool-option
              (GenericObjectPool. (make-connection-factory spec))
              opts))))

(defn close-pool [{:keys [^ObjectPool pool]}] (.close pool))

(defn pooled-conn [{:keys [^ObjectPool pool]}]
      (.borrowObject pool))

(defn release-conn [{:keys [^ObjectPool pool]} conn]
      (.returnObject pool conn))

(defmacro wcar
          "
          The ConnectionPool returned from conn-pool must be the first argument to this macro
          Evaluates body in the context.
          Sends Redis commands to server as pipeline and returns the
          server's response. Releases connection back to pool when done.

          `conn-opts` arg is a map with connection pool and spec options:
            {:pool {} :spec {:host \"127.0.0.1\" :port 6379}} ; Default
            {:pool {} :spec {:uri \"redis://redistogo:pass@panga.redistogo.com:9475/\"}}
            {:pool {} :spec {:host \"127.0.0.1\" :port 6379
                             :password \"secret\"
                             :timeout-ms 6000
                             :db 3}}

          A `nil` or `{}` `conn-opts` will use defaults. A `:none` pool can be used
          to skip connection pooling (not recommended).
          For other pool options, Ref. http://goo.gl/e1p1h3,
                                       http://goo.gl/Sz4uN1 (defaults).

          See also `with-replies`."
          {:arglists '([conn-opts :as-pipeline & body] [conn-opts & body])}
          ;; [conn-opts & [s1 & sn :as sigs]]
          [^ObjectPool pool & sigs]
          `(let [conn# (pooled-conn ~pool)

                 ;; To support `wcar` nesting with req planning, we mimic
                 ;; `with-replies` stashing logic here to simulate immediate writes:
                 ?stashed-replies#
                 (when protocol/*context*
                       (protocol/execute-requests :get-replies :as-pipeline))]

                (try
                  (let [response# (protocol/with-context conn#
                                                         (protocol/with-replies* ~@sigs))]
                       (release-conn ~pool conn#)
                       response#)

                  (catch Throwable t# ; nb Throwable to catch assertions, etc.
                    (release-conn ~pool conn#) (throw t#))

                  ;; Restore any stashed replies to preexisting context:
                  (finally
                    (when ?stashed-replies#
                          (car/parse nil ; Already parsed on stashing
                                 (mapv car/return ?stashed-replies#)))))))

(def ^:private lkey (partial car/key :carmine :lock))

(defn acquire-lock
  "Attempts to acquire a distributed lock, returning an owner UUID iff successful."
  ;; TODO Waiting on http://goo.gl/YemR7 for simpler (non-Lua) solution
  [pool lock-name timeout-ms wait-ms]
  (let [max-udt (+ wait-ms (System/currentTimeMillis))
        uuid    (str (java.util.UUID/randomUUID))]
    (wcar pool ; Hold one connection for all attempts
          (loop []
            (when (> max-udt (System/currentTimeMillis))
              (if (-> (car/lua
                        "if redis.call('setnx', _:lkey, _:uuid) == 1 then
                          redis.call('pexpire', _:lkey, _:timeout-ms)
                          return 1
                        else
                          return 0
                        end"
                        {:lkey       (lkey lock-name)}
                        {:uuid       uuid
                         :timeout-ms timeout-ms})
                      car/with-replies car/as-bool)
                (car/return uuid)
                (do (Thread/sleep 1) (recur))))))))

(defn release-lock
  "Attempts to release a distributed lock, returning true iff successful."
  [pool lock-name owner-uuid]
  (wcar pool
        (car/parse-bool
          (car/lua
            "if redis.call('get', _:lkey) == _:uuid then
               redis.call('del', _:lkey)
               return 1
             else
               return 0
             end"
            {:lkey (lkey lock-name)}
            {:uuid owner-uuid}))))

(defn have-lock? [pool lock-name owner-uuid]
  (= (wcar pool (car/get (lkey lock-name))) owner-uuid))

(defmacro with-lock
  "Attempts to acquire a distributed lock, executing body and then releasing
  lock when successful. Returns {:result <body's result>} on successful release,
  or nil if the lock could not be acquired. If the lock is successfully acquired
  but expires before being released, throws an exception."
  [pool lock-name timeout-ms wait-ms & body]
  `(let [pool# ~pool]                                       ;pool lock-name timeout-ms wait-ms
     (when-let [uuid# (acquire-lock pool# ~lock-name ~timeout-ms ~wait-ms)]
       (try
         {:result (do ~@body)} ; Wrapped to distinguish nil body result
         (catch Throwable t# (throw t#))
         (finally
           (when-not (release-lock pool# ~lock-name uuid#)
             (throw (ex-info (str "Lock expired before it was released: "
                                  ~lock-name)
                             {:lock-name ~lock-name}))))))))

(defn- release-all-locks! [pool]
  (when-let [lkeys (seq (wcar pool (car/keys (lkey :*))))]
    (wcar pool (apply car/del lkeys))))
