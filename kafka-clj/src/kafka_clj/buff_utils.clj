(ns kafka-clj.buff-utils
  (:import [io.netty.buffer ByteBuf]))

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
  (let [pos (.writerIndex buff)]
    (.writeInt buff (int -1))
    (apply f buff args)
    (.setInt buff (int pos) (- (.writerIndex buff) pos 4))))
      