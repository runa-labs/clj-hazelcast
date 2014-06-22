(ns clj-hazelcast.test.query-test
  (:import (clojure.lang IPersistentMap)
           (com.hazelcast.nio.serialization Serializer StreamSerializer)
           (com.hazelcast.config InMemoryFormat MapConfig))
  (:use [clojure.test])
  (:require
    [clj-hazelcast.core :as hz]
    [clj-hazelcast.query :as q]))

(def ned {:fname "Ned" :lname "Stark" :address "Winterfell"})
(def robb {:fname "Robb" :lname "Stark" :address "Winterfell"})

(def query-map (atom nil))

(defn fixture [f]
  (hz/init)
  (reset! query-map (hz/get-map "cljhazelcast.query-tests.query-map"))
  (hz/put! @query-map :k1 ned)
  (hz/put! @query-map :k2 robb)
  (f))

(use-fixtures :once fixture)


(q/defpredicate ned? [k v] (.equals (:fname v) "Ned"))
(q/defpredicate stark? [k v] (.equals (:lname v) "Stark"))
(q/defpredicate stannis? [k v] (.equals (:fname v) "Stannis"))


(deftest predicate-test
  (testing "predicate"
    (is (= 1
           (count (q/values @query-map ned?))))
    (is (= 2
           (count (q/values @query-map stark?))))
    (is (= 0
           (count (q/values @query-map stannis?))))
    ))


