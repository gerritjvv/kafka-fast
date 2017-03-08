(ns
  ^{:author "gerritjvv"
    :doc    "
    Simple Direct TCP Client for the producer
    The producers sit behind an async buffer where data is pushed on
    by multiple threads, the TCP sending itself does not need yet another layer or indirection
    which at the current moment under high loads with huge messages can cause out of memory errors

    Usage
    (def client (tcp/tcp-client \"localhost\" 7002))
    (tcp/write! client \"one two three\" :flush true)
    (tcp/read-async-loop! client (fn [^bytes bts] (prn (String. bts))))
    (tcp/close! client)

     Provides a pooled connection via tcp-pool the config options supported are from GenericKeyedObjectPoolConfig

    "}
  kafka-clj.tcp
  (:require
    [clojure.tools.logging :refer [error info debug enabled?]]
    [kafka-clj.pool.keyed :as pool-keyed]
    [kafka-clj.pool.api :as pool-api]
    [kafka-clj.jaas :as jaas]
    [clj-tuple :refer [tuple]]
    [tcp-driver.io.pool :as tcp-pool]
    [tcp-driver.driver :as tcp-driver]
    [tcp-driver.routing.retry :as retry]
    [tcp-driver.routing.policy :as routing]
    [tcp-driver.io.conn :as tcp-conn])
  (:import (java.net Socket SocketException InetSocketAddress)
           (java.io InputStream OutputStream BufferedInputStream BufferedOutputStream DataInputStream)
           (io.netty.buffer ByteBuf Unpooled)
           (kafka_clj.util IOUtil)
           (javax.security.auth.login LoginContext)
           (javax.security.sasl SaslClient)
           (tcp_driver.io.pool IPool)
           (kafka_clj.pool.api PoolObj)
           (java.util.concurrent.atomic AtomicInteger)))

(defrecord SASLCtx [^LoginContext login-ctx ^SaslClient sasl-client])

(defrecord TCPClient [host port conf socket ^BufferedInputStream input ^BufferedOutputStream output sasl-ctx])

(defn closed? [{:keys [^Socket socket sasl-ctx]}]
  (if sasl-ctx
    (cond
      (.isClosed socket) true
      (jaas/jaas-expired? (:login-ctx sasl-ctx))  (do       ;;expired but socket is open, close it.
                                                    (.close socket)
                                                    true))
    (.isClosed socket)))

(defn open-socket ^Socket [host port {:keys [timeout-ms] :or {timeout-ms 10000}}]
  (let [socket (Socket.)]
    (.setSendBufferSize socket (int (* 1048576 2)))
    (.setReceiveBufferSize socket (int (* 1048576 2)))

    (.connect socket (InetSocketAddress. (str host) (int port)) (long timeout-ms))

    socket))

(defn check-sasl
  "Returns c assoc :sasl-ctx where sasl-ctx contains or not the connected sasl connection if jaas is specified"
  [{:keys [jaas kafka-version] :as conf} fqdn conn]
  (let [sasl-ctx (when jaas
                   (let [c (jaas/jaas-login jaas)

                         sasl-client (jaas/sasl-client conf c fqdn)]

                     (jaas/with-auth c                      ;;need to run handshake inside the subject doAs method
                                     #(jaas/sasl-handshake! conn sasl-client 30000 :kafka-version kafka-version))
                     (->SASLCtx c sasl-client)))]
    (assoc conn :sasl-ctx sasl-ctx)))


(defn tcp-client
  "Creates a tcp client from host port and conf
   InputStream is DataInputStream(BufferedInputStream) and output is BufferedOutputStream

   if jaas is specified it must point to a configuration section on the jaas and kerberos files defined as env properties
   -Djava.security.auth.login.config=/vagrant/vagrant/config/kafka_client_jaas.conf
   -Djava.security.krb5.conf=/vagrant/vagrant/config/krb5.conf

   if jaas is specified and kafka 0.9.0 is used add :kafka-version \"0.9.0\" to the conf
   "
  [host port conf]
  {:pre [(string? host) (number? port)]}
  (let [socket (open-socket host port conf)

        tcp-client (->TCPClient host port conf socket
                                (DataInputStream. (BufferedInputStream. (.getInputStream socket)))
                                (BufferedOutputStream. (.getOutputStream socket))
                                nil)

        fqdn (.getCanonicalHostName
               (.getInetAddress socket)) ]

    (check-sasl conf fqdn tcp-client)))

(defn ^ByteBuf wrap-bts
  "Wrap a byte array in a ByteBuf"
  [^"[B" bts]
  (Unpooled/wrappedBuffer bts))

(defn read-int ^long [^DataInputStream input ^long timeout]
  (long (IOUtil/readInt input timeout)))

(defn read-bts ^"[B" [^DataInputStream input ^long timeout ^long cnt]
  (IOUtil/readBytes input (int cnt) timeout))


(def ^"[B" read-response kafka-clj.tcp-api/read-response)

(def write! kafka-clj.tcp-api/write!)

(defn wrap-exception [f & args]
  (try
    (apply f args)
    (catch Exception e
      (do
        (.printStackTrace e)
        (error (str "Ignored Exception " e) e)
        nil))))

(defn close! [{:keys [^Socket socket ^InputStream input ^OutputStream output sasl-ctx]}]
  {:pre [socket]}
  (try
    (when (not (.isClosed socket))
      (wrap-exception #(.flush output))
      (wrap-exception #(.close output))
      (wrap-exception #(.close input))
      (wrap-exception #(.close socket))
      (when sasl-ctx
        (.dispose ^SaslClient (:sasl-client sasl-ctx))))
    (catch Throwable t
      (error (str "Ignored exception " t) t))))

(defn- _write-bytes [tcp-client ^"[B" bts]
  (.write ^BufferedOutputStream (:output tcp-client) bts))


(defn borrow
  ([obj-pool host port]
   (borrow obj-pool host port 10000))
  ([obj-pool host port timeout-ms]
   (pool-api/poll obj-pool (tuple host port) timeout-ms)))

(defn pool-obj-val
  "Returns the PoolObj contained value"
  [pooled-obj]
  (pool-api/pool-obj-val pooled-obj))

(defn release [obj-pool host port v]
  (pool-api/return obj-pool (tuple host port) v))

(defn close-pool! [obj-pool]
  (pool-api/close-all obj-pool))

(extend-protocol kafka-clj.tcp-api/TCPWritable

  (Class/forName "[B")
  (-write! [obj tcp-client]
    (_write-bytes tcp-client obj))

  ByteBuf
  (-write! [obj tcp-client]
    (let [^ByteBuf buff obj
          readable-bytes (.readableBytes buff)]
      (.readBytes buff ^OutputStream (:output tcp-client) (int readable-bytes))))

  String
  (-write! [obj tcp-client]
    (_write-bytes tcp-client (.getBytes ^String obj "UTF-8"))))

;;(defprotocol ITCPConn
;(-input-stream [this])
;(-output-stream [this])
;(-close [this])
;(-valid? [this]))

;(defrecord TCPClient [host port conf socket ^BufferedInputStream input ^BufferedOutputStream output sasl-ctx])

(extend-protocol tcp-conn/ITCPConn

  ;;driver returns a pool that returns IPool
  PoolObj
  (-input-stream [this]
    (tcp-conn/-input-stream (pool-obj-val this)))

  (-output-stream [this]
    (tcp-conn/-output-stream (pool-obj-val this)))

  (-close [this]
    (tcp-conn/-close (pool-obj-val this)))

  (-valid? [this]
    (tcp-conn/-valid? (pool-obj-val this)))

  TCPClient
  (-input-stream [this]
    (:input this))

  (-output-stream [this]
    (:output this))

  (-close [this]
    (close! this))

  (-valid? [this]
    (not (closed? this))))

(defn fqn [^Socket socket]
  (.getCanonicalHostName
    (.getInetAddress socket)))

(defn init-tcp-conn [conf conn]
  (check-sasl conf (fqn (:socket conn)) conn))

(defn driver
  "
     if jaas is specified in pool-conf it must point to a configuration section on the jaas and kerberos files defined as env properties
        -Djava.security.auth.login.config=/vagrant/vagrant/config/kafka_client_jaas.conf
        -Djava.security.krb5.conf=/vagrant/vagrant/config/krb5.conf

        if jaas is specified and kafka 0.9.0 is used add :kafka-version \"0.9.0\" to the conf
  "
  [hosts & {:keys [routing-conf pool-conf retry-limit] :or {retry-limit 2 routing-conf {} pool-conf {}}}]
  (tcp-driver/create-default hosts
                             :pool-conf (merge pool-conf {:post-create-fn #(init-tcp-conn pool-conf (:conn %))})
                             :routing-conf routing-conf
                             :retry-limit retry-limit))
