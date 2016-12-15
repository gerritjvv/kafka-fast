(ns
  ^{:doc "USAGE:
            Use for JAAS Kerberos Plain Text


            (def tcp-client (... create tcp client ...)
            (def c (jaas/jaas-login \"KafkaClient\"))
            (def sasl-client (jaas/sasl-client c (jaas/principal-name c) broker-host))
            (jaas/sasl-handshake! tcp-client sasl-client timeout-ms)"}

  kafka-clj.jaas
  (:import (javax.security.auth.login LoginContext)
           (javax.security.auth Subject)
           (java.security PrivilegedExceptionAction Principal)
           (javax.security.sasl Sasl SaslClient)
           (javax.security.auth.callback CallbackHandler)
           (java.util.concurrent TimeoutException)
           (io.netty.buffer ByteBuf Unpooled)
           (javax.security.auth.kerberos KerberosTicket)
           (java.io InputStream ByteArrayInputStream ByteArrayOutputStream)
           (sun.misc HexDumpEncoder))
  (:require [clojure.tools.logging :refer [info error debug]]
            [kafka-clj.tcp-api :as tcp-api]
            [kafka-clj.protocol :as protocol]
            [kafka-clj.buff-utils :as buff-utils]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;; helper functions

(defonce ^:constant MECHS (into-array String ["GSSAPI"]))

(defn timeout? [timeout-ms current-ms]
  (> (- (System/currentTimeMillis) (long current-ms)) (long timeout-ms)))

(defn with-auth [^LoginContext ctx f]
  (Subject/doAs ^Subject (.getSubject ctx)
                (reify PrivilegedExceptionAction
                  (run [_]
                    (f)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;; public functions

(defn jaas-login ^LoginContext
[jaas-name]
  (debug "trying jaas login for name " jaas-name)
  (let [login (LoginContext. (str jaas-name))]
    (.login login)
    (debug "login complete: have " login)
    login))

(defn jaas-expire-time [^LoginContext ctx]
  (let [ticket (first (.getPrivateCredentials (.getSubject ctx) KerberosTicket))]
    (.getAuthTime ticket)))

(defn jaas-expired? [^LoginContext ctx]
 false)

(defn jaas-logout [^LoginContext ctx]
  (.logout ctx))

(defn ^Principal principal-name
  "Return the unparsed principal name e.g kafka/broker1.kafkafast@KAFKAFAST
   A context can have multiple principals, this method returns the first principal found"
  [^LoginContext ctx]
  (.getName ^Principal (first (.getPrincipals ^Subject (.getSubject ctx)))))

(defn kafka-principal-name
  "A kafka principal i.e servicePrinipal contains kafka/host e.g kafka/broker1.kafkafast"
  [broker-host]
  (str "kafka/" broker-host))

;;with auth
(defn ^SaslClient sasl-client
  "See https://docs.oracle.com/javase/8/docs/api/javax/security/sasl/Sasl.html
   servicePrincipal: should be in the format kafka/{host} see kafka-principal-name
   host: the kafka broker"
  [^LoginContext ctx host]
  (let []
    (with-auth ctx (fn []
                     (Sasl/createSaslClient MECHS
                                            (principal-name ctx)
                                            (str "kafka")   ;;the sasl client will construct kafka/{host}@{REALM}
                                            (str host)
                                            {}
                                            (reify CallbackHandler
                                              (handle [_ callbacks]
                                                (prn "Callback " callbacks))))))))

(defn as-hex [^"[B" bts]
  (when bts
    (let [cnt (Math/min (int 16) (int (count bts)))
          arr-out (ByteArrayOutputStream.)
          bts-in (ByteArrayInputStream. bts (int 0) (int cnt))]


      (.encodeBuffer (HexDumpEncoder.) bts-in arr-out)

      (.toString arr-out))))

(defn send-read-data
  "Write [int size][client-resp] then read [int size][server resp]"
  [client ^"[B" client-resp should-read]
  (debug "gssapi send to broker : " (as-hex client-resp))

  (tcp-api/write-request! client client-resp)

  (when should-read
    (let [resp (tcp-api/read-response client)]
      (debug "gssapi read from broker : " (as-hex resp))
      resp)))

(defn handshake-loop! [client ^SaslClient sasl-client timeout-ms]
  {:pre [client sasl-client (number? timeout-ms)]}

  (let [current-ms (System/currentTimeMillis)]

    (loop [server-resp (byte-array []) initial true]
      (cond
        (timeout? timeout-ms current-ms) (throw (TimeoutException. (str "Could not handshake with sasl client " sasl-client " in " timeout-ms "ms")))

        (.isComplete sasl-client) true

        :else (let [client-resp (if (and
                                      initial
                                      (not (.hasInitialResponse sasl-client)))
                                  (byte-array [])           ;;send initial empty byte array token
                                  (.evaluateChallenge sasl-client server-resp)) ;otherwise eval and send response back
                    ]

               (if client-resp                              ;;if any client-resp data, send, but only if sasl-client is not complete
                 (recur (send-read-data client client-resp (not (.isComplete sasl-client))) false)
                 (recur server-resp false)))))))


(defn handshake-request!
  "
   SaslHandshake API (Key: 17)
    1. SizeInBytes => int16
    2. api_key => INT16      (0)    17
    3. api_version => INT16  (0)
    4. correlation_id => INT32
    5. client_id => NULLABLE_STRING
    6. mechanism => String  \"GSSAPI\" or \"PLAIN\";
  "
  [client]
  (let [corr-id (protocol/unique-corrid!)]
    (prn "Write request: " (short protocol/API_KEY_SASL_HANDSHAKE)
         " version " (int protocol/API_VERSION)
         " mechs " (str (first MECHS))
         " corr-id " corr-id)

    (tcp-api/write! client
                    (buff-utils/with-size (Unpooled/buffer)
                                          (fn [^ByteBuf buff]
                                            (.writeShort buff (short protocol/API_KEY_SASL_HANDSHAKE))
                                            (.writeShort buff (short protocol/API_VERSION))
                                            (.writeInt   buff (int corr-id))
                                            (buff-utils/write-short-string buff nil)
                                            (buff-utils/write-short-string buff (str (first MECHS)))))
                    :flush true)))

(defn handshake-response!
  ";Response:
    1. SizeInBytes => int16
    2. correlation_id => INT32
    3. error_code => INT16
       0 => None, 34 => InvalidSaslState, 35 => UnsupportedVersion
    4. enabled_mechanisms => [STRING]"
  [client]
  (let [buff (Unpooled/wrappedBuffer (tcp-api/read-response client))

        corr-id  (.readInt buff)
        error-code (.readShort buff)

        mechs (buff-utils/read-string-array buff)]
    (prn "handshake-response: error-code: " error-code " mechs " mechs " corr-id " corr-id)
    (if (zero? error-code)
      true
      (throw (RuntimeException. (str "Handshake error: " error-code " mechanims: " mechs))))))

(defn sasl-handshake!
  "client: kafka-clj/tcp client
   sasl-client: jaas/sasl-client
   timeout-ms: timeout in milliseconds"
  [client ^SaslClient sasl-client timeout-ms]
  (handshake-request! client)
  (handshake-response! client)

  ;;no exception means handshake is complete
  (handshake-loop! client sasl-client timeout-ms))

