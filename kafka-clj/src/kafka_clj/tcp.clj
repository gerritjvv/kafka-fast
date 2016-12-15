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
            [clj-tuple :refer [tuple]])
  (:import (java.net Socket SocketException)
           (java.io InputStream OutputStream BufferedInputStream BufferedOutputStream DataInputStream)
           (io.netty.buffer ByteBuf Unpooled)
           (kafka_clj.util IOUtil)
           (javax.security.auth.login LoginContext)
           (javax.security.sasl SaslClient)))

(defrecord SASLCtx [^LoginContext login-ctx ^SaslClient sasl-client])

(defrecord TCPClient [host port conf socket ^BufferedInputStream input ^BufferedOutputStream output sasl-ctx])

(defn closed? [{:keys [^Socket socket sasl-ctx]}]
  (if sasl-ctx
    (or (.isClosed socket)
        ;;check sasl-ctx tickets
        (jaas/jaas-expired? (:login-ctx sasl-ctx)))
    (.isClosed socket)))

(defn open-socket ^Socket [host port]
  (let [socket (Socket. (str host) (int port))]
    (.setSendBufferSize socket (int (* 1048576 2)))
    (.setReceiveBufferSize socket (int (* 1048576 2)))

    socket))

(defn tcp-client
  "Creates a tcp client from host port and conf
   InputStream is DataInputStream(BufferedInputStream) and output is BufferedOutputStream

   if jaas is specified it must point to a configuration section on the jaas and kerberos files defined as env properties
   -Djava.security.auth.login.config=/vagrant/vagrant/config/kafka_client_jaas.conf
   -Djava.security.krb5.conf=/vagrant/vagrant/config/krb5.conf
   "
  [host port & {:keys [jaas] :as conf}]
  {:pre [(string? host) (number? port)]}
  (let [socket (open-socket host port)

        tcp-client (->TCPClient host port conf socket
                                (DataInputStream. (BufferedInputStream. (.getInputStream socket)))
                                (BufferedOutputStream. (.getOutputStream socket))
                                nil)

        fqdn (.getCanonicalHostName
               (.getInetAddress socket))

        sasl-ctx (when jaas
                   (let [c (jaas/jaas-login jaas)

                         sasl-client (jaas/sasl-client conf c fqdn)]

                     (jaas/with-auth c                      ;;need to run handshake inside the subject doAs method
                                     #(jaas/sasl-handshake! tcp-client sasl-client 30000))
                     (->SASLCtx c sasl-client)))]

    (assoc tcp-client :sasl-ctx sasl-ctx)))

(defn ^ByteBuf wrap-bts
  "Wrap a byte array in a ByteBuf"
  [^"[B" bts]
  (Unpooled/wrappedBuffer bts))

(defn read-int ^long [^DataInputStream input ^long timeout]
  (long (IOUtil/readInt input timeout)))

(defn read-bts ^"[B" [^DataInputStream input ^long timeout ^long cnt]
  (IOUtil/readBytes input (int cnt) timeout))


(def ^"[B" read-response kafka-clj.tcp-api/read-response)

(defn closed-exception?
  "Return true if the exception contains the word closed, otherwise nil"
  [^Exception e]
  (.contains (.toString e) "closed"))

(defn read-async-loop!
  "Only call this once on the tcp-client, it will create a background thread that exits when the socket is closed.
   The message must always be [4 bytes size N][N bytes]"
  [{:keys [^Socket socket ^DataInputStream input] :as conn} handler]
  {:pre [socket input (fn? handler)]}
  (future
    (try
      (while (not (closed? conn))
        (try
          (handler (read-response conn))
          (catch Exception e
            ;;only print out exceptions during debug
            (debug "Timeout while reading response from producer broker " e))))
      (catch SocketException e nil))))

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


(defn tcp-pool
  "Note that objects returned from this pool are PoolObj instances and to get at the tcp-conn we
   need to use pool-obj-val"
  [conf]
  (pool-keyed/create-keyed-obj-pool conf
                                    (fn [conf [host port]] (wrap-exception #(apply tcp-client host port (flatten (seq conf))))) ;;create-f
                                    (fn [_ _ v] (try
                                                  (not (closed? (pool-api/pool-obj-val v)))
                                                  (catch Exception e (do
                                                                       (error (str "Ignored Exception " e) e)
                                                                       false)))) ;;validate-f
                                    (fn [_ _ v]
                                      (wrap-exception close! (pool-api/pool-obj-val v))) ;;destroy-f
                                    ))

(defn borrow
  ([obj-pool host port]
   (borrow obj-pool host port 10000))
  ([obj-pool host port timeout-ms]
   (pool-api/poll obj-pool (tuple host port) timeout-ms)))

(defn pool-obj-val
  "Returns the PoolObj contained value"
  [pooled-obj]
  (pool-api/pool-obj-val pooled-obj))

(defn invalidate! [obj-pool host port v]
  (let [k (tuple host port)]
    (pool-api/return obj-pool k v)
    (pool-api/close-all obj-pool k)))


(defn release [obj-pool host port v]
  (pool-api/return obj-pool (tuple host port) v))

(defn close-pool! [obj-pool]
  (pool-api/close-all obj-pool))

(defn ^ByteBuf empty-byte-buff []
  (Unpooled/buffer))

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

