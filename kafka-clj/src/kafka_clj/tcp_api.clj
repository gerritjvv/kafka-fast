(ns
  ^{:doc "remove secular dependancies from jaas to tcp"}
  kafka-clj.tcp-api
  (:import (kafka_clj.util IOUtil)
           (java.io OutputStream BufferedOutputStream DataInputStream)))

(defprotocol TCPWritable (-write! [obj tcp-client] "Write obj to the tcp client"))


(defn write! [tcp-client obj & {:keys [flush] :or {flush false}}]
  (when obj
    (-write! obj tcp-client)
    (if flush
      (.flush ^BufferedOutputStream (:output tcp-client)))))

(defn write-request!
  "Helper function that writes [int size in bytes][bytes] to the client and flus the request"
  [{:keys [^OutputStream output] :as client} bts]
  {:pre [output (instance? OutputStream output) bts]}
  (let [cnt (count bts)]
    (IOUtil/writeInt output (int cnt))
    (write! client bts :flush true)))

(defn ^"[B" read-response
  "Read a single response from the DataInputStream of type [int length][message bytes]
   The message bytes are returned as a byte array
   Throws SocketException, Exception"
  ([k]
   (read-response {} k 30000))
  ([wu {:keys [^DataInputStream input]} ^long timeout]
   (let [len (long (IOUtil/readInt input timeout))
         bts (IOUtil/readBytes input (int len) timeout)]
     bts)))