(ns clj-hazelcast.test.core-with-config
  (:use [clojure.test])
  (:require
   [clj-hazelcast.core :as hazelcast])
  (:import
   (com.hazelcast.config XmlConfigBuilder Config TcpIpConfig)))

(def test-map (atom nil))

(def localhost-config
  (let [config (.. (XmlConfigBuilder.) build)]
    (.. config getNetworkConfig getJoin getMulticastConfig
        (setEnabled false))
    (.. config getNetworkConfig getJoin getTcpIpConfig
        (setEnabled true)
        (addMember "127.0.0.1"))
    config))      

(defn fixture [f]
  (hazelcast/init localhost-config)
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
