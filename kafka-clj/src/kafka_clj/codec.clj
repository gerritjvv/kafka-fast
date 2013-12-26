(ns kafka-clj.codec
  
  (:import [java.io DataOutputStream ByteArrayOutputStream OutputStream]
           [java.util.zip GZIPOutputStream]
           [org.iq80.snappy SnappyOutputStream Snappy]
           [kafka_clj.util Util]))


(defonce SNAPPY "snappy")
(defonce GZIP "gzip")

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

(defn snappy-out []
  (let [out (ByteArrayOutputStream.)]
    [(DataOutputStream. (SnappyOutputStream. out)) out] ))

(defn ^bytes compress [codec ^bytes bts]
  (cond 
    (= codec 1) (let [[^DataOutputStream dout ^ByteArrayOutputStream bout] (gzip-out)]
                  (.write dout bts (int 0) (int (count bts)))
                  (.close dout)
                  (.close bout)
                  (.toByteArray bout))
    (= codec 2) (Snappy/compress bts)))
                

(defn get-compress-out [codec]
  (cond 
    (= codec 1) (gzip-out)
    (= codec 2) (snappy-out)
    :else (throw (RuntimeException. (str "Codec " codec " not supported please use none, gzip or snappy") ))))