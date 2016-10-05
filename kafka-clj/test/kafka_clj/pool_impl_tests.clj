(ns kafka-clj.pool-impl-tests
  (require
           [kafka-clj.pool.api :refer :all]
           [kafka-clj.pool.keyed :refer :all]
           [kafka-clj.pool.util :refer :all]
           [clojure.test.check :as tc]
           [clojure.test.check.generators :as gen]
           [clojure.test.check.properties :as prop]
           [midje.sweet :refer :all])
  (:import (java.util.concurrent TimeoutException Semaphore Executors TimeUnit ExecutorService CountDownLatch)
           (java.util HashMap Map)))

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

;;;;;;;;;;;;;;;;;
;;;; util functions specific to the test-multiple-threads tests
(defn with-pool-keyed-obj
  "Get a pooled object at key k, call (f obj) then return obj to the keyed pool at key k,
   this function's return value is that of f"
  [keyed-pool k f]
  (let [obj (poll keyed-pool k)]
    (try
      (f (pool-obj-val obj))
      (finally
        (return keyed-pool k obj)))))

(defn set-inc-cnt!
  "Requires a mutable map and calls get inc put with the key :cnt get :cnt"
  [^Map m]
  (.put m :cnt (inc (.get m :cnt)))
  (.get m :cnt))

(defn get-cnt-val-from-pool
  "Get the :cnt value for a key k, otherwise -1 is returned, the object is returned to the pool after its usage
   we assume a pool of 1"
  [keyed-pool k]
  (with-pool-keyed-obj keyed-pool k (fn [^Map m] (get m :cnt -1))))

(defn all-java-futures-completes? [futures]
  (every? #(and (.isDone %) (.get %)) futures))

(defn merge-max-cnt-maps
  "merge {:k <k> :cnt <int-count>} maps with max"
  [cnt-maps]
  (let [merge-fn (fn [state {:keys [k cnt]}]
                   (update state k (fn [cnt2] (if cnt2 (max cnt cnt2) cnt))))]

    (reduce merge-fn {} cnt-maps)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; test mock functions

(defn poll-pool-objs
  "Run poll on pool with key k limit times and return a list of the results"
  [pool limit k]
  (reduce (fn [v _] (conj v (poll pool k))) [] (range limit)))

(defn return-pool-objs
  "Run return on pool with key k limit times and return a list of the results"
  [pool k objs]
  (doseq [o objs]
    (return pool k o)))


(defn create-test-pool
  "Create a random value pool with limit as pool-limit"
  [limit]
  (create-keyed-obj-pool {:pool-limit limit}
                         (identity-f (rand))
                         (identity-f true)
                         (identity-f nil)))


(defn create-f-rand [& _] (rand))
(def validate-f-true (identity-f true))
(def validate-f-false (identity-f false))
(def destroy-f-noop (identity-f nil))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; test functions

(defn test-a-pool-acquire-release []
  (prop/for-all [v (gen/vector gen/keyword 2 5)
                 limit (gen-rand-int)]
                (let [pool (create-test-pool limit)

                      pool-size-equals-limit? #(= (avaiable? pool %) limit)]

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
                    (expect-timeout-exception (poll pool :a 200))))))


(defn test-keyed-pool-fns []
  (prop/for-all [v (gen/such-that not-empty (gen/vector gen/keyword))]
                (let [ctx nil
                      m-atom (atom {})

                      create-f create-f-rand
                      validate-f validate-f-true
                      destroy-f  destroy-f-noop
                      create-pool-f (fn [_ _] (create-keyed-obj-pool nil create-f validate-f destroy-f))

                      get-results (doall (filter not-nil? (for [k v]
                                                            (get-keyed-pool ctx m-atom k create-pool-f))))]


                  (and
                    (= (count v) (count get-results))
                    (= (count (distinct v)) (count (keys @m-atom)))))))

(defn test-keyed-obj-pool []
  (prop/for-all [v (gen/such-that not-empty (gen/vector gen/keyword))]
                (let [create-f create-f-rand
                      validate-f validate-f-true
                      destroy-f destroy-f-noop
                      keyed-pool (create-keyed-obj-pool nil create-f validate-f destroy-f)

                      get-results (doall (filter not-nil? (for [k v]
                                                            (poll keyed-pool k))))]


                  (and
                    (= (count v) (count get-results))))))



(defn test-timeout-no-valid-object []
  (prop/for-all [v (gen/such-that not-empty (gen/vector gen/keyword))]
                (let [create-f create-f-rand
                      validate-f validate-f-false
                      destroy-f destroy-f-noop
                      pool-limit 10
                      keyed-pool (create-keyed-obj-pool {:pool-limit pool-limit} create-f validate-f destroy-f)
                      k (first v)]

                  (close-all keyed-pool)
                  ;;;we expect a timeout exception because validate-f always returns false

                  (and
                    (expect-timeout-exception (poll keyed-pool k 100))
                    (= (avaiable? keyed-pool k) pool-limit)))))

(defn test-remove-idle []
  (prop/for-all [v (gen/such-that not-empty (gen/vector gen/keyword 2 5))]


                (let [
                      ^CountDownLatch count-latch (CountDownLatch. (count v))
                      created-atoms (ref {})
                      ;;each create-f rerturns atom(1) and each destroy decrements the value
                      ;;values are added during create to the created-atom ref, such that after
                      ;; destroy-f has been called the atom at that key in created-atoms should be 0
                      create-f (fn [_ k] (let [a (atom 1)]
                                           (dosync
                                             (alter created-atoms assoc k a))
                                           a))

                      validate-f validate-f-true
                      destroy-f (fn [_ _ v]
                                  (swap! (pool-obj-val v) dec)
                                  (.countDown count-latch))

                      managed-pool (create-managed-keyed-obj-pool {:idle-limit-ms 100
                                                                   :idle-check-freq-ms 100}
                                                                  (create-keyed-obj-pool {:pool-limit 1} create-f validate-f destroy-f))]

                  (doseq [k v]
                    ;;cause objects to be created and returned
                    (with-pool-keyed-obj managed-pool k (fn [v] (assert (not-nil? v)))))

                  ;;wait one second for objs to expire
                  (prn "idle waiting on latch")
                  (.await count-latch 60 TimeUnit/SECONDS)
                  (prn "idle got latch")
                  (prn "count v: " (count v) " = " (count (keys @created-atoms)) " keys " (mapv str (keys @created-atoms)))
                  (prn "vals " (clojure.string/join "," (mapv deref (vals @created-atoms))))

                  (try
                    (and
                      (= (count v) (count (keys @created-atoms)))
                      (every?
                        zero?
                        (for [[_ cnt] @created-atoms]
                          @cnt)))
                    (finally
                      (close-all managed-pool))))))

(defn test-multiple-threads []
  (prop/for-all [v (gen/such-that not-empty (gen/vector gen/keyword 2 5))
                 th-len (gen/choose 5 20)
                 th-it (gen/choose 1000 10000)]

                ;;keyed pool that will create for each key the number 0 hidden in a mutable hash map under the key :cnt (we need a mutable set because we want to test that the object pool doesn't
                ;;  leak same instances to other threads)
                ;; the tasks will select a random key, then increment the number 1 th-it times, each increment will get a pool object and release it.
                ;; sometimes the increment will sleep a random amount of time to simulate pauses and longer usage
                ;; each task returns the count it thinks the key should have with its key. {:k key :cnt count}
                ;;
                ;; to check we merge the results of the different futures and max on the count,
                ;; the compare this value with what is in the pool.
                (let [create-f (fn [_ _] (doto (HashMap.) (.put :cnt 0)))

                      validate-f  validate-f-true
                      destroy-f destroy-f-noop

                      keyed-pool (create-keyed-obj-pool {:pool-limit 1} create-f validate-f destroy-f)

                      pool-keys (into [] (take 5 v))
                      ^ExecutorService exec (Executors/newCachedThreadPool)

                      test-fn (fn []
                                (let [k (rand-nth pool-keys)
                                      cnt (last (repeatedly th-it #(with-pool-keyed-obj keyed-pool k set-inc-cnt!)))]
                                  {:k k :cnt cnt}))

                      futures (.invokeAll exec (repeat th-len test-fn))]

                  (.shutdown exec)
                  (.awaitTermination exec 30 TimeUnit/SECONDS)

                  ;;check all futures have completed
                  (assert (all-java-futures-completes? futures))

                  (let [merged-maps (merge-max-cnt-maps (mapv #(.get %) futures))]
                    (every?
                      true?
                      (for [[k cnt] merged-maps]
                        (= cnt (get-cnt-val-from-pool keyed-pool k))))))))


(defn create-k-pool []
  (let [create-f (fn [_ _] (doto (HashMap.) (.put :cnt 0)))

        validate-f  validate-f-true
        destroy-f destroy-f-noop

        keyed-pool (create-keyed-obj-pool {:pool-limit 1} create-f validate-f destroy-f)]

    keyed-pool))


(def test-cases [                                           ;test-a-pool-limit
                 ;test-multiple-threads
                 ;test-keyed-pool-fns
                 ;test-keyed-obj-pool
                 ;test-a-pool-acquire-release
                 ;test-timeout-no-valid-object
                 ;test-a-pool-limit
                 test-remove-idle
                 ])

(defn run-test-cases
  "For each test case in redis-test-cases run the function with quikc-check and return the result"
  []
  (doall
    (for [test-case test-cases]
      (let [res (:result (tc/quick-check 10 (test-case)))]
        (prn test-case " => " res)
        res))))

(facts "pool impl tests"
       (every? true? (run-test-cases)) => true)