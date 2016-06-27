(ns kafka-clj.pool-impl-tests
  (require [kafka-clj.pool-impl :as pool-impl]
           [clojure.test.check :as tc]
           [clojure.test.check.generators :as gen]
           [clojure.test.check.properties :as prop])
  (:import (java.util.concurrent TimeoutException Semaphore)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; util functions

(def not-nil? #(not (nil? %)))

(defn gen-rand-int []
  (gen/fmap (fn [_]
              (int (+ (rand 100) 10))) gen/s-pos-int))

(defmacro always
  [& body]
  `(fn [& _#]
     ~@body))

(defmacro expect-timeout-exception [& body]
  `(try
     ~@body
     (catch TimeoutException _# true)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; test mock functions

(defn poll-pool-objs
  "Run poll on pool with key k limit times and return a list of the results"
  [pool limit k]
  (reduce (fn [v _] (conj v (pool-impl/poll pool k))) [] (range limit)))

(defn return-pool-objs
  "Run return on pool with key k limit times and return a list of the results"
  [pool k objs]
  (doseq [o objs]
    (pool-impl/return pool k o)))


(defn create-test-pool
  "Create a random value pool with limit as pool-limit"
  [limit]
  (pool-impl/create-atom-keyed-obj-pool {:pool-limit limit}
                                        (always (rand))
                                        (always true)
                                        (always nil)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; test functions

(defn test-a-pool-acquire-release []
  (prop/for-all [v (gen/vector gen/keyword 2 5)
                 limit (gen-rand-int)]
                (let [pool (create-test-pool limit)

                      pool-size-equals-limit? #(= (pool-impl/avaiable? pool %) limit)]

                  ;;we test that we can poll and return all objects without hanging on polling
                  ;;and then check the sempahore output
                  (doseq [k v]
                    (return-pool-objs pool k (poll-pool-objs pool limit k)))

                  (every? pool-size-equals-limit? v))))

(defn test-a-pool-limit []
  (prop/for-all [limit (gen-rand-int)]

                (let [pool (create-test-pool limit)
                      ;;poll all objects
                      polled-objs (poll-pool-objs pool limit :k)

                      ^Semaphore sem (-> pool :m-atom deref :k :ctx :sem)]


                  (prn "pool-counts " (count polled-objs) " sem " sem)

                  (and
                    (zero? (.availablePermits sem))
                    (expect-timeout-exception (pool-impl/poll pool :a 200))))))


(defn test-keyed-pool-fns []
  (prop/for-all [v (gen/vector gen/keyword)]
                (let [ctx nil
                      m-atom (atom {})

                      create-f (fn [_] (rand))
                      validate-f identity
                      destroy-f identity
                      create-pool-f (fn [_] (pool-impl/create-atom-keyed-obj-pool nil create-f validate-f destroy-f))

                      get-results (doall (filter not-nil? (for [k v]
                                                            (pool-impl/get-keyed-pool ctx m-atom k create-pool-f))))]


                  (and
                    (= (count v) (count get-results))
                    (= (count (distinct v)) (count (keys @m-atom)))))))

(defn test-keyed-obj-pool []
  (prop/for-all [v (gen/vector gen/keyword)]
                (let [create-f (fn [_] (rand))
                      validate-f identity
                      destroy-f identity
                      keyed-pool (pool-impl/create-atom-keyed-obj-pool nil create-f validate-f destroy-f)

                      get-results (doall (filter not-nil? (for [k v]
                                                            (pool-impl/poll keyed-pool k))))]


                  (and
                    (= (count v) (count get-results))))))



(def test-cases [test-a-pool-limit
                 test-keyed-pool-fns
                 test-keyed-obj-pool
                 test-a-pool-acquire-release])

(defn run-test-cases
  "For each test case in redis-test-cases run the function with quikc-check and return the result"
  []
  (doall
    (for [test-case test-cases]
      (do
        (:result (tc/quick-check 10 (test-case)))))))