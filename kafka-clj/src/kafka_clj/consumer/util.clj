(ns kafka-clj.consumer.util
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :refer [error]])
  (:import  [java.util HashMap ArrayList Arrays]))

(defn ^Long ciel-index
  "Calculate the index of offset and len combined, which is offset + (dec len)"
  ([{:keys [^Long offset ^Long len]}]
   (+ offset (dec len)))
  ([^Long offset ^Long len]
   (+ offset (dec len))))



(defn- get-create-array! [^HashMap state k]
  (if-let [arr (.get state k)]
    arr
    (let [arr (long-array 24 -1)]
      (.put state k arr)
      arr)))

(defn- ^long get-array-val!
  "Get a value at an array index, if the index is bigger than the array the array is grown
   and put into the state at key k"
  [^longs arr ^HashMap state k  ^long index]
  (try
    (aget arr index)
    (catch IndexOutOfBoundsException e (let [arr2 (Arrays/copyOf arr (+ index 10))] ;grow the array
                                         (Arrays/fill arr2 -1)
                                         (.put state k arr2)
                                         (aget arr2 index)
                                         ))))

(defn- accept-msg!?
  ""
  [^HashMap state {:keys [topic ^long partition ^long offset] :as msg}]
  (let [^longs arr (get-create-array! state topic)
         ^long v (get-array-val! arr state topic partition)]
    (if (< v offset)
      (do
        (aset arr partition offset)
        state)
      (do
        (error "duplicate " msg  " state: " (Arrays/toString arr)  " : v " v)
        nil))))

(defn copy-new-messages
  "Creates a go block that will only copy new messages to the channel,
   New messages are messages with a higher offset
   This code copies fast, in itself the acceps-msg!? function does more than 12million p/s"
  [ch-from ch-to]
  ;internally we use long arrays and mutated state, but this is inside a single thread context
  ;and not seen by the outside wolrd, making the code still thread safe.
  (async/go
    (loop [state (HashMap.)]
      (when-let [msg (async/<! ch-from)]
        (let [state2 (accept-msg!? state msg)]
          (if state2
            (do
              (async/>! ch-to msg)
              (recur state2))
            (recur state)))))))
