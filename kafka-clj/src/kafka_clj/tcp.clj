(ns
  ^{:author "gerritjvv"
    :doc "
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
  (:require [clojure.tools.logging :refer [error info]]
            [kafka-clj.pool :as pool]
            [clj-tuple :refer [tuple]])
  (:import (java.net Socket SocketException)
           (java.io IOException InputStream OutputStream BufferedInputStream BufferedOutputStream DataInputStream)
           (io.netty.buffer ByteBuf Unpooled)
           (kafka_clj.util Util IOUtil)
           (java.util.concurrent TimeoutException)))


(defrecord TCPClient [host port conf socket ^BufferedInputStream input ^BufferedOutputStream output])

(defprotocol TCPWritable (-write! [obj tcp-client] "Write obj to the tcp client"))


(defn closed? [{:keys [^Socket socket]}]
  (.isClosed socket))

(defn tcp-client
  "Creates a tcp client from host port and conf
   InputStream is DataInputStream(BufferedInputStream) and output is BufferedOutputStream"
  [host port & conf]
  {:pre [(string? host) (number? port)]}
  (let [socket (Socket. (str host) (int port))]
    (.setSendBufferSize socket (int (* 1048576 2)))
    (.setReceiveBufferSize socket (int (* 1048576 2)))

    (->TCPClient host port conf socket
                 (DataInputStream. (BufferedInputStream. (.getInputStream socket)))
                 (BufferedOutputStream. (.getOutputStream socket)))))

(defn ^ByteBuf wrap-bts
  "Wrap a byte array in a ByteBuf"
  [^"[B" bts]
  (Unpooled/wrappedBuffer bts))

(defn read-int ^long [^DataInputStream input ^long timeout]
  (long (IOUtil/readInt input timeout)))

(defn read-bts ^"[B" [^DataInputStream input ^long timeout ^long cnt]
  (IOUtil/readBytes input (int cnt) timeout))

(defn ^"[B" read-response
  "Read a single response from the DataInputStream of type [int length][message bytes]
   The message bytes are returned as a byte array
   Throws SocketException, Exception"
  ([k]
    (read-response {} k 30000))
  ([wu {:keys [^DataInputStream input]} ^long timeout]
   (let [len (read-int input timeout)
         bts (read-bts input timeout len)]
     bts)))

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
          (catch TimeoutException e nil)
          (catch IOException e (when-not (closed-exception? e) (error e)))
          (catch SocketException e nil)
          (catch Exception e (error e e))))
      (catch SocketException e nil))))

(defn write! [tcp-client obj & {:keys [flush] :or {flush false}}]
  (when obj
    (-write! obj tcp-client)
    (if flush
      (.flush ^BufferedOutputStream (:output tcp-client)))))

(defn close! [{:keys [^Socket socket ^InputStream input ^OutputStream output]}]
  {:pre [socket]}
  (when (not (.isClosed socket))
    (.flush output)
    (.close output)
    (.close input)
    (.close socket)))

(defn- _write-bytes [tcp-client ^"[B" bts]
  (.write ^BufferedOutputStream (:output tcp-client) bts))

(defn tcp-pool [conf]
  (pool/object-pool
    (pool/keyed-obj-factory
      (fn [[host port] conf] (apply tcp-client host port (flatten (seq conf))))
      (fn [v conf] (not (closed? v)))
      (fn [v conf] (close! v))
      conf)
    conf))

(defn borrow
  ([obj-pool host port]
    (borrow obj-pool host port 10000))
  ([obj-pool host port timeout-ms]
   (pool/borrow obj-pool (tuple host port) timeout-ms)))

(defn release [obj-pool host port v]
  (pool/release obj-pool (tuple host port) v))

(defn close-pool! [obj-pool]
  (pool/close! obj-pool))

(defn ^ByteBuf empty-byte-buff []
  (Unpooled/buffer))

(extend-protocol TCPWritable
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

