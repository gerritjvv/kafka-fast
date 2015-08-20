(ns kafka-clj.io-utils-tests
  (:import [kafka_clj.util IOUtil]
           (java.io InputStream ByteArrayInputStream)
           (java.util.concurrent TimeoutException))
  (:require [midje.sweet :refer :all]))


(defn seq-input-stream
  "Not thread safe and uses simple trickery to return either till (first s) returns nil, or
   the number of bytes returned is equal to cnt
   s must be a sequence
   cnt the number of items to return
   avail-chunk if any avail this number will be returned, otherwise zero
   return a InputStream"
  [s cnt avail-chunk & {:keys [avail-sleep] :or {avail-sleep 100}}]
  (let [counter (atom 0)
        seq-ref (atom s)
        last-ts (atom 0)]
    (proxy [InputStream] []
           (available []
             (if (< (- (System/currentTimeMillis) @last-ts) avail-sleep)
               0
               (if (and (<= @counter cnt) (first @seq-ref))
                 (do
                   (reset! last-ts (System/currentTimeMillis))
                   avail-chunk)
                 0)))
           (read
             ([bts start len]
               (let [bts2 (byte-array (take-while #(not (= -1 %)) (take len (repeatedly #(.read this)))))]
                 (System/arraycopy bts (int start) bts2 0 (Math/min (count bts2) len))
                 (count bts2)))
             ([]
              (if (>= @counter cnt)
                -1
                (let [[f & rs] @seq-ref]
                  (if f
                    (do
                      (reset! seq-ref rs)
                      (swap! counter inc)
                      (byte f))
                    -1))))))))

(fact "Test read N bytes from mock input stream"
      (let [^InputStream in (seq-input-stream (repeat 1) 5 10)]
        (count (take-while #(not (= -1 %)) (repeatedly #(.read in)))) => 5))

(fact "Test read bytes from IOUtil with sleep under timeout"
      (let [expected-bts (int 1000)
            avail-chunk 500
            timeout-ms 10000
            ^InputStream in (seq-input-stream (repeatedly (fn []
                                                            (Thread/sleep 10)
                                                            1))
                                              (* 2 expected-bts)
                                              avail-chunk
                                              :avail-sleep 100)

            ^"[B" bts (IOUtil/readBytes in (int expected-bts) timeout-ms)]

        (count bts) => expected-bts))

(fact "Test read bytes from IOUtil with sleep over timeout"
      (let [expected-bts (int 1000)
            avail-chunk 10
            timeout-ms 1000
            ^InputStream in (seq-input-stream (repeatedly (fn [] 1))
                                              (* 2 expected-bts)
                                              avail-chunk
                                              :avail-sleep 10000)]

        (try
          (do
            (IOUtil/readBytes in (int expected-bts) timeout-ms)
            false => true)
          (catch TimeoutException e true => true))))