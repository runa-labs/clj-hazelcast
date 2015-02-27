(ns clj-hazelcast.test.core-with-instance
  (:use [clojure.test])
  (:require
   [clj-hazelcast.core :as hazelcast])
  (:import
   com.hazelcast.core.Hazelcast
   com.hazelcast.core.HazelcastInstance))

(defonce ^:const map-name "clj-hazelcast.cluster-tests-with-instance.test-map")

(def hazelcast-instance (atom nil))

(defn fixture [f]
  (reset! hazelcast-instance (Hazelcast/newHazelcastInstance))
  (hazelcast/with-instance @hazelcast-instance)
  (f)
  (hazelcast/shutdown))

(use-fixtures :once fixture)

(deftest ensure-provided-instance-used
  "Putting via clj-hazelcast should put in the provided Hazelcast instance"
  (let [instance-map (.getMap ^HazelcastInstance @hazelcast-instance map-name)
        clj-hazelcast-map (hazelcast/get-map map-name)]
    (is (= 456
           (do
             (hazelcast/put! clj-hazelcast-map :bar 456)
             (get instance-map :bar))))))
