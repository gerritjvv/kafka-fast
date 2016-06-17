(ns
  ^{:doc "Test the single instance redis implementation"}
  kafka-clj.redis-tests
  (:require [kafka-clj.test-utils :as test-utils]
            [kafka-clj.redis.protocol :as redis-protocol]
            [kafka-clj.consumer.node :as node]
            [kafka-clj.redis.core :as redis]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop])
  (:use midje.sweet))

;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; utils functions

(defn gen-string []
  (gen/such-that (complement clojure.string/blank?) gen/string-alphanumeric))

(defn gen-vector
  ([] (gen-vector 10))
  ([n]
   (gen/not-empty (gen/vector-distinct (gen-string) {:num-elements n}))))

(defn random-sub-vec
  "Take a random sub sample of the vector always half the size of the vector v"
  [v]
  (into [] (comp (filter (complement nil?)) (mapcat identity) (take (Math/abs (double (/ (count v) 2))))) (repeatedly #(random-sample 0.5 v))))

(defn push-vector [redis-conn queue-name v]
  (redis-protocol/-wcar redis-conn
                        #(do
                          (redis-protocol/-wcar redis-conn (fn [] (redis-protocol/-flushall redis-conn)))
                          (doseq [item v]
                            (redis-protocol/-lpush redis-conn queue-name item)))))

(defn remove-and-get-vec
  "Remove each item in v from the queue-name"
  [redis-conn queue-name v]
  (redis-protocol/-wcar redis-conn
                        #(doseq [item v]
                          (redis-protocol/-lrem redis-conn queue-name 1 item))))

(defn get-queue-items [redis-conn queue-name]
  (redis-protocol/-wcar redis-conn #(redis-protocol/-lrange redis-conn queue-name 0 1000000)))

(defn redis-llen [redis-conn queue-name]
  (redis-protocol/-wcar redis-conn
                        #(redis-protocol/-llen redis-conn queue-name)))

;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; Test functions

(defn copy-redis-queue
  "Test that the copy queue command empties the source queue"
  [redis-conn]
  (prop/for-all [v-n (gen-vector 100)
                 queue-src (gen-string)
                 n (gen/choose 10 60)]
                (try
                  (let [v (mapv #(array-map % 1) v-n)
                        queue-dest (str queue-src (System/currentTimeMillis))
                        _ (push-vector redis-conn queue-src v)
                        queue-len-src1 (redis-llen redis-conn queue-src)
                        _ (node/copy-redis-queue redis-conn queue-src queue-dest)

                        queue-len-src2 (redis-llen redis-conn queue-src)
                        queue-len-dest (redis-llen redis-conn queue-dest)]

                    (prn "copy-res " [queue-len-src1 queue-len-src2 queue-len-dest])
                    (and (= queue-len-src1 100)
                         (zero? queue-len-src2)
                         (= queue-len-src1 queue-len-dest)))
                  (catch Exception e (.printStackTrace e)))))

(defn brpoplpush
  "Test that the brpoplpush function works"
  [redis-conn]
  (prop/for-all [v (gen-vector 100)
                 queue-src (gen-string)
                 n (gen/choose 10 60)]
                (try
                  (let [queue-dest (str queue-src (System/currentTimeMillis))]
                    (push-vector redis-conn queue-src v)

                    (let [_ (dotimes [_ n]
                              (redis-protocol/-wcar redis-conn
                                                    (fn []
                                                      (redis-protocol/-brpoplpush redis-conn queue-src queue-dest 10000))))

                          queue-len-src (redis-llen redis-conn queue-src)
                          queue-len-dest (redis-llen redis-conn queue-dest)]

                      (and
                        (= queue-len-src (- (count v) n))
                        (= queue-len-dest n))))
                  (catch Exception e (.printStackTrace e)))))
(defn lrem
  "Test that the lpush function works"
  [redis-conn]
  (prop/for-all [v-s (gen-vector 100)
                 queue-name (gen-string)]
                (try


                  (let [v (mapv #(array-map % 1) v-s)
                        _ (push-vector redis-conn queue-name v)
                        sub-v (random-sub-vec v)
                        new-vec (do
                                  (remove-and-get-vec redis-conn queue-name sub-v)
                                  (get-queue-items redis-conn queue-name))]
                    (=
                      (clojure.set/difference (into #{} v) (into #{} sub-v))
                      (into #{} new-vec)))
                  (catch Exception e (.printStackTrace e)))))


(defn lpush-llen
  "Test tath the lpush function works"
  [redis-conn]
  (prop/for-all [v-s (gen-vector)
                 queue-name (gen-string)]
                (try
                  (let [v (mapv #(vector % 1) v-s)
                        _ (push-vector redis-conn queue-name v)

                        [result result-cnt] (redis-protocol/-wcar redis-conn
                                                                  #(vector (redis-protocol/-lrange redis-conn queue-name 0 (count v))
                                                                           (redis-protocol/-llen redis-conn queue-name)))]

                    ;;test that the result from lrange is equal to v and the count from llen is equals to (count v)
                    (if-not (and (= (sort v) (sort result)) (= result-cnt (count v)))
                      (prn (sort v) " != " (sort result))
                      true))

                  (catch Exception e (.printStackTrace e)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;; Test runs and framework functions

;;; test cases to run in run-test-cases
(def redis-test-cases [
                       lrem
                       lpush-llen
                       brpoplpush
                       copy-redis-queue])

(defn run-test-cases
  "For each test case in redis-test-cases run the function with quikc-check and return the result"
  [redis redis-conn]
  (doall
    (for [test-case redis-test-cases]
      (do
        (:result (tc/quick-check 1 (test-case redis-conn)))))))

(defn run-single-redis-tests
  "For each function in redis-test-cases run it with a redis-connection"
  []
  (test-utils/with-redis (fn [redis]
                           (every? true? (run-test-cases redis (redis/create-single-conn {:host "localhost" :port (:port redis)}))))))

(defn run-cluster-redis-tests
  "For each function in redis-test-cases run it with a redis-connection"
  []
  (test-utils/with-redis-cluster
    (fn [redis]
      (every? true? (run-test-cases redis (redis/redis-cluster-conn {:host (:hosts redis)}))))))


(facts "Test redis single impl"
       (run-single-redis-tests) => true)

(facts "Test redis cluster impl"
       (run-cluster-redis-tests) => true)