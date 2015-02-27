(ns clj-hazelcast.test.core
  (:use [clojure.test])
  (:require
   [clj-hazelcast.core :as hazelcast]))

(def test-map (atom nil))

(defn fixture [f]
  (hazelcast/init)
  (reset! test-map (hazelcast/get-map "clj-hazelcast.cluster-tests.test-map"))
  (f)
  (hazelcast/shutdown))

(use-fixtures :once fixture)

(deftest map-test
  (is (= 1
         (do
           (hazelcast/put! @test-map :foo 1)
           (:foo @test-map))))
  (is 
   (let [m {:a [1 "two"] :b #{:three 'four}}]
     (= m
        (do
          (hazelcast/put! @test-map :bar m)
          (:bar @test-map))))))

(deftest listeners
  (is (= 2
         (let [events (atom [])
               listener-fn (fn [& event]
                             (swap! events conj event))
               listener (hazelcast/add-entry-listener! @test-map listener-fn)
               result (do
                        (hazelcast/put! @test-map :baz "foobar")
                        (hazelcast/put! @test-map :foo "bizbang")
                        (Thread/sleep 1000)
                        (count @events))]
           (hazelcast/remove-entry-listener! @test-map listener)
           result))))


(deftest queue
  (testing "blocking queue add and take"
    (is (= 42 (let [queue (hazelcast/get-queue "test-queue")]
                   (hazelcast/add! queue {:my-map-value 42})
                   (:my-map-value (hazelcast/take! queue))))))
  (testing "blocking queue addAll and take"
    (is (= #{42 52} (let [queue (hazelcast/get-queue "test-queue2")
                          result (atom #{})]
                (hazelcast/add-all! queue [{:my-map-value 42} {:my-map-value 52}])
                (swap! result conj (:my-map-value (hazelcast/take! queue)))
                (swap! result conj (:my-map-value (hazelcast/take! queue))))))))

(deftest put-ttl-test
  (is (= "will expire"
         (do
           (hazelcast/put-ttl! @test-map :key-ttl "will expire" 1)
           (:key-ttl @test-map))))
  (Thread/sleep 2000)
  (is (nil? (:key-ttl @test-map))))
