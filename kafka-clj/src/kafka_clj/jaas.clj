(ns
  ^{:doc "USAGE:
            Use for JAAS Kerberos Plain Text


            (def tcp-client (... create tcp client ...)
            (def c (jaas/jaas-login \"KafkaClient\"))
            (def sasl-client (jaas/sasl-client c (jaas/principal-name c) broker-host))
            (jaas/sasl-handshake! tcp-client sasl-client timeout-ms)

            System environment config must be set, see the project.clj file for this project.
            Properties required are:

            -Djava.security.auth.login.config=/vagrant/vagrant/config/kafka_client_jaas.conf
            -Djava.security.krb5.conf=/vagrant/vagrant/config/krb5.conf
            "}

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

           (sun.misc HexDumpEncoder)
           (kafka_clj.util Util))
  (:require [clojure.tools.logging :refer [info error debug]]
            [kafka-clj.tcp-api :as tcp-api]
            [kafka-clj.protocol :as protocol]
            [tcp-driver.driver :as tcp-driver]
            [tcp-driver.io.stream :as tcp-stream]
            [kafka-clj.buff-utils :as buff-utils]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;; helper functions

(defonce ^:constant MECHS (into-array String ["GSSAPI"]))

(defonce ^:constant ^Long HALF-MINUTE-MS (* 30 1000))

(defn timeout? [timeout-ms current-ms]
  (> (- (System/currentTimeMillis) (long current-ms)) (long timeout-ms)))

(defn with-auth [^LoginContext ctx f]
  (Subject/doAs ^Subject (.getSubject ctx)
                (reify PrivilegedExceptionAction
                  (run [_]
                    (f)))))

(defn readp-resp [conn timeout-ms]
  (let [len (tcp-stream/read-int conn timeout-ms)]
    (tcp-stream/read-bytes conn len timeout-ms)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;; public functions

(defn kafka-service-name
  "Search for sasl.kerberos.service.name is defined in either conf, System.properties its used,
   otherwise the default kafka name is used"
  [conf]
  (or
    (:sasl.kerberos.service.name conf)
    (System/getProperty "sasl.kerberos.service.name")
    "kafka"))


(defn jaas-login ^LoginContext
[jaas-name]
  (debug "trying jaas login for name " jaas-name)
  (let [login (LoginContext. (str jaas-name))]
    (.login login)
    (debug "login complete: have " login)
    login))

(defn jaas-expire-time
  "For all KerberosTicket(s) in the LoginContext the min auth time value is returned"
  [^LoginContext ctx]
  (let [^Subject subject (.getSubject ctx)]
    (if subject
      (apply
        min
        (map
          #(.getTime (.getEndTime ^KerberosTicket %))
          (.getPrivateCredentials subject KerberosTicket)))
      Long/MAX_VALUE)))

(defn jaas-expired?
  "True if the expire time is withing 30 seconds of the current time"
  [^LoginContext ctx]
  (debug "jaas-expired: expire-time " (long (jaas-expire-time ctx))  " curr-time " (System/currentTimeMillis))
  (<
    (- (long (jaas-expire-time ctx))
       (System/currentTimeMillis))

    HALF-MINUTE-MS))

(defn jaas-logout [^LoginContext ctx]
  (.logout ctx))

(defn ^Principal principal-name
  "Return the unparsed principal name e.g kafka/broker1.kafkafast@KAFKAFAST
   A context can have multiple principals, this method returns the first principal found"
  [^LoginContext ctx]
  (.getName ^Principal (first (.getPrincipals ^Subject (.getSubject ctx)))))

;;with auth
(defn ^SaslClient sasl-client
  "See https://docs.oracle.com/javase/8/docs/api/javax/security/sasl/Sasl.html
   servicePrincipal: should be in the format kafka/{host} see kafka-principal-name
   host: the kafka broker"
  [conf ^LoginContext ctx host]
  (let []
    (with-auth ctx (fn []
                     (Sasl/createSaslClient MECHS
                                            (principal-name ctx)
                                            (str (kafka-service-name conf)) ;;the sasl client will construct kafka/{host}@{REALM}
                                            (str host)
                                            {}
                                            (reify CallbackHandler
                                              (handle [_ callbacks]
                                                (debug "Callback " callbacks))))))))

(defn as-hex [^"[B" bts]
  (when bts
    (let [cnt (Math/min (int 16) (int (count bts)))
          arr-out (ByteArrayOutputStream.)
          bts-in (ByteArrayInputStream. bts (int 0) (int cnt))]


      (.encodeBuffer (HexDumpEncoder.) bts-in arr-out)

      (.toString arr-out))))

(defn send-read-data
  "Write [int size][client-resp] then read [int size][server resp]"
  [conn ^"[B" client-resp should-read timeout-ms]
  (debug "gssapi send to broker : " (as-hex client-resp))

  (tcp-stream/write-int conn (count client-resp))
  (tcp-stream/write-bytes conn client-resp)
  (tcp-stream/flush-out conn)

  (when should-read
    (let [resp (readp-resp conn timeout-ms)]
      (debug "gssapi read from broker : " (as-hex resp))
      resp)))

(defn handshake-loop! [conn ^SaslClient sasl-client timeout-ms]
  {:pre [conn sasl-client (number? timeout-ms)]}

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

                (if client-resp                             ;;if any client-resp data, send, but only if sasl-client is not complete
                  (recur (send-read-data conn client-resp (not (.isComplete sasl-client)) timeout-ms) false)
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
  [conn]
  (let [corr-id (protocol/unique-corrid!)
        buff (Unpooled/buffer)

        _ (do
            (buff-utils/with-size buff
                                  (fn [^ByteBuf buff]
                                    (.writeShort buff (short protocol/API_KEY_SASL_HANDSHAKE))
                                    (.writeShort buff (short protocol/API_VERSION))
                                    (.writeInt buff (int corr-id))
                                    (buff-utils/write-short-string buff nil)
                                    (buff-utils/write-short-string buff (str (first MECHS))))))]

    (info "Write request: " (short protocol/API_KEY_SASL_HANDSHAKE)
           " version " (int protocol/API_VERSION)
           " mechs " (str (first MECHS))
           " corr-id " corr-id)

    (tcp-stream/write-bytes conn (Util/toBytes ^ByteBuf buff))
    (tcp-stream/flush-out conn)))

(defn handshake-response!
  ";Response:
    1. SizeInBytes => int16
    2. correlation_id => INT32
    3. error_code => INT16
       0 => None, 34 => InvalidSaslState, 35 => UnsupportedVersion
    4. enabled_mechanisms => [STRING]"
  [conn timeout-ms]
  (let [len  (tcp-stream/read-int conn timeout-ms)
        buff (Unpooled/wrappedBuffer (tcp-stream/read-bytes conn len timeout-ms))

        corr-id (.readInt buff)
        error-code (.readShort buff)

        mechs (buff-utils/read-string-array buff)]
    (info "handshake-response: error-code: " error-code " mechs " mechs " corr-id " corr-id)
    (if (zero? error-code)
      true
      (throw (RuntimeException. (str "Handshake error: " error-code " mechanims: " mechs))))))

(defn sasl-handshake!
  "client: kafka-clj/tcp client
   sasl-client: jaas/sasl-client
   timeout-ms: timeout in milliseconds"
  [conn ^SaslClient sasl-client timeout-ms & {:keys [kafka-version] :or {kafka-version "0.10.0"}}]

  (debug "sasl-handshake!: >>>>>>>>>>>>>>>>>>>>>>>>kafka-version: " kafka-version)

  (when (not (.contains (str kafka-version) "0.9"))
    (handshake-request! conn)
    (handshake-response! conn timeout-ms))

  ;;no exception means handshake is complete
  (handshake-loop! conn sasl-client timeout-ms))

