(ns kafka-clj.codec
  
  (:import [java.io DataOutputStream ByteArrayOutputStream OutputStream ByteArrayInputStream]
           [java.util.zip GZIPOutputStream GZIPInputStream]
           [kafka_clj.util Util]
           [java.util Arrays]))


(defonce SNAPPY "snappy")
(defonce GZIP "gzip")
(defonce GZIP_NATIVE "gzip-native")

(defn crc32-int
  "CRC for byte array."
  [^bytes ba]
  (Util/crc32 ba))

(defn get-codec-int [codec]
  (cond 
    (= codec "gzip") 1
    (= codec "snappy") 2
    (= codec "none") 0
    :else (throw (RuntimeException. (str "Codec " codec " not supported")))))

(defn- gzip-out []
  (let [out (ByteArrayOutputStream.)]
    [(DataOutputStream. (GZIPOutputStream. out)) out] ))


(defn ^"[B" compress [codec ^"[B" bts]
  (cond 
    (= codec 1) (let [[^DataOutputStream dout ^ByteArrayOutputStream bout] (gzip-out)]
                  (.write dout bts (int 0) (int (count bts)))
                  (.close dout)
                  (.close bout)
                  (.toByteArray bout))
    (= codec 2) (Util/compressSnappy bts) ))
                

(defn ^"[B" uncompress [codec ^"[B" bts]
  (cond 
    (= codec 1) (Util/deflateGzip bts)
    (= codec 2) (Util/deflateSnappy bts))) ;skip the snappy header which is 8 bytes



