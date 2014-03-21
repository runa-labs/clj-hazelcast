(ns clj-hazelcast.test.mr-test
  (:use clojure.test)
  (:require [clj-hazelcast.core :as hz]
            [clj-hazelcast.mr :as mr]
            [clojure.tools.logging :as log])
  )


(def mr-test-map (atom nil))

(defn fixture [f]
  (hz/init)
  (reset! mr-test-map (hz/get-map "clj-hazelcast.cluster-tests.mr-test-map"))
  (f))

(use-fixtures :once fixture)


(deftest mapreduce-test
  (testing "count"
    (is (= {:k1 1 :k2 1 :k5 1 :k3 1 :k4 1}
           (let [m (fn [k v] (do
                               (log/infof "Mapped Key %s Mapped Value %s " k v) [k 1]))
                 r (fn [v state] (let [val (:val @state)
                                       old @state]
                                   (log/infof "Reducing %s" old)
                                   (if-not (nil? val)
                                     (reset! state (assoc old :val (inc val)))
                                     (reset! state (assoc old :val 1))
                                     )
                                   (log/infof "Reduced %s" @state)
                                   )
                     )]
             (hz/put! @mr-test-map :k1 "v1")
             (hz/put! @mr-test-map :k2 "v2")
             (hz/put! @mr-test-map :k3 "v3")
             (hz/put! @mr-test-map :k4 "v4")
             (hz/put! @mr-test-map :k5 "v5")
             (let [tracker (mr/make-job-tracker @hz/hazelcast)
                   res (mr/submit-job @mr-test-map m r tracker)]
               (log/infof "Result %s" res)
               res))))))