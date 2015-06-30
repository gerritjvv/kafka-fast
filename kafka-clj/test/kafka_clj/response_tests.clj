(ns kafka-clj.response-tests
  (:require [kafka-clj.response :as response])
  (:use midje.sweet
        conjure.core)
  (:import (java.io DataOutputStream ByteArrayOutputStream DataInputStream ByteArrayInputStream)))


(defn write-short-string
  "Helper function to write a short string"
  [^DataOutputStream out ^String s]
  (let [bts (.getBytes s "UTF-8")]
    (doto out
      (.writeShort (short (count bts)))
      (.write bts))))

(defn write-partition-resp
  "Write a partition response to the output"
  [^DataOutputStream out partition error-code offset]
  (doto out
    (.writeInt (int partition))
    (.writeShort (short error-code))
    (.writeLong (long offset))))

(defn write-topic-resp
  "partition-responses = [{:partition <int> :error-code <short> :offset <long>}]"
  [^DataOutputStream out topic partition-responses]
  (write-short-string out topic)
  (.writeInt out (count partition-responses))

  (doseq [{:keys [partition error-code offset]} partition-responses]
    (write-partition-resp out partition error-code offset)))

(defn write-response!
  "Write a message response simulating what kafka would write when ack > 0
  topics = {:topic-name [{:partition <int> :error-code <short> :offset <long>}]}
  returns = the DataOutputStream that the data was written to"
  ([corr-id topics]
   (let [out (ByteArrayOutputStream.)]
     (write-response! (DataOutputStream. out) corr-id topics)
     out))
  ([^DataOutputStream out corr-id topics]
   {:pre [(number? corr-id) (map? topics)]}
   (.writeInt out (int corr-id))
   (.writeInt out (int (count (keys topics))))

   (doseq [[topic-name partition-responses] topics]
     (write-topic-resp out topic-name partition-responses))
   out))

(defn write-test-response! []
  (let [out (write-response! 1 {"test1" [{:partition 1 :error-code 0 :offset 100}
                                         {:partition 2 :error-code 8 :offset 200}]
                                "test2" [{:partition 1 :error-code 0 :offset 100}
                                         {:partition 2 :error-code 8 :offset 200}]})]
    (.close out)
    out))

(facts "Test response values"
       (let [out (write-test-response!)
             resp (response/in->kafkarespseq (DataInputStream. (ByteArrayInputStream. (.toByteArray out))))]

         resp => truthy
         (coll? resp) => true
         (nth resp 0) => {:correlation-id 1, :topic "test1", :partition 1, :error-code 0, :offset 100}
         (nth resp 1) => {:correlation-id 1, :topic "test1", :partition 2, :error-code 8, :offset 200}
         (nth resp 2) => {:correlation-id 1, :topic "test2", :partition 1, :error-code 0, :offset 100}
         (nth resp 3) => {:correlation-id 1, :topic "test2", :partition 2, :error-code 8, :offset 200}))
