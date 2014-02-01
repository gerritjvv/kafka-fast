(ns kafka-clj.buff-utils
  (:import [io.netty.buffer ByteBuf]))


(defonce ^:constant compression-code-mask 0x03)


(defn ^ByteBuf inc-capacity [^ByteBuf bytebuf l]
  (let [len (+ (.capacity bytebuf) (int l))]
    (if (> len (.maxCapacity bytebuf))
      (.capacity bytebuf len))
    bytebuf))

(defn read-short-string [^ByteBuf buff]
  (let [size (.readShort buff)]
    (if (pos? size)
      (let [arr  (byte-array size)]
		    (.readBytes buff arr)
		    (String. arr "UTF-8")))))

(defn ^ByteBuf write-short-string [^ByteBuf buff s]
  (-> buff
    (.writeShort (short (count s)))
    (.writeBytes (.getBytes (str s) "UTF-8"))))

(defn with-size [^ByteBuf buff f & args]
  "apply f and then write len-bytes-written-by-f bytes-written-by-f
   and returns the result returned by f"
  (let [pos (.writerIndex buff)]
    (.writeInt buff (int -1))
    (let [resp (apply f buff args)]
       (.setInt buff (int pos) (- (.writerIndex buff) pos 4))
       resp)))

(defn read-byte-array [^ByteBuf buff]
  (let [len (.readInt buff)
        arr (byte-array (if (pos? len) len 0))]
    (.readBytes buff arr)
    arr))

(defn codec-from-attributes [attributes-byte]
  ;buffer.get(AttributesOffset) & CompressionCodeMask)
  (bit-and attributes-byte compression-code-mask))

