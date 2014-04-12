(ns clj-hazelcast.test.mr-test
  (:import (java.util.concurrent TimeoutException TimeUnit))
  (:use clojure.test)
  (:require [clj-hazelcast.core :as hz]
            [clj-hazelcast.mr :as mr]
            [clojure.tools.logging :as log])
  )


(def mr-test-map (atom nil))
(def wordcount-map (atom nil))
(def validation-map (atom nil))

(defn fixture [f]
  (hz/init)
  (reset! mr-test-map (hz/get-map "clj-hazelcast.cluster-tests.mr-test-map"))
  (reset! wordcount-map (hz/get-map "clj-hazelcast.cluster-tests.wordcount-map"))
  (reset! validation-map (hz/get-map "clj-hazelcast.cluster-tests.validation-map"))
  (f))

(use-fixtures :once fixture)

(defn split-words [text]
  "split text into a list of words"
  (re-seq #"\w+" text))

(defn calculate-frequencies [words]
  "convert list of words to a word-frequency hash"
  (reduce (fn [words word] (assoc words word (inc (get words word 0))))
          {}
          words))

(deftest mapreduce-test
  (testing "key-count"
    (is (= {:k1 1 :k2 1 :k5 1 :k3 1 :k4 1}
           (let [m (fn [k v] [[k 1]])
                 r (fn [v state] (let [old @state]
                                   (if-not (nil? (:val old))
                                     (reset! state (assoc old :val (inc (:val old))))
                                     (reset! state (assoc old :val 1)))))]
             (hz/put! @mr-test-map :k1 "v1")
             (hz/put! @mr-test-map :k2 "v2")
             (hz/put! @mr-test-map :k3 "v3")
             (hz/put! @mr-test-map :k4 "v4")
             (hz/put! @mr-test-map :k5 "v5")
             (let [tracker (mr/make-job-tracker @hz/hazelcast)
                   fut (mr/submit-job {:map @mr-test-map :mapper-fn m :reducer-fn r :tracker tracker})
                   res (.get fut 2 TimeUnit/SECONDS)]
               (log/infof "Result %s" res)
               res))))))

(defn mapper
  [k v] (let [freq (calculate-frequencies (split-words v))]
          (first (partition 2 freq))))

(deftest wordcount-test
  (testing "mapper"
    (is (= [["bar" 1] ["foo" 1]] (mapper "some-key" "foo bar"))))
  (testing "wordcount"
    (is (= {"clojure" 3 "java" 2 "lisp" 1}
           (let [
                  r (fn [v state] (let [old @state]
                                    (if-not (nil? (:val old))
                                      (reset! state (assoc old :val (inc (:val old))))
                                      (reset! state (assoc old :val 1)))))]
             (hz/put! @wordcount-map :k1 "clojure java")
             (hz/put! @wordcount-map :k2 "java clojure")
             (hz/put! @wordcount-map :k3 "lisp clojure")
             (let [tracker (mr/make-job-tracker @hz/hazelcast)
                   fut (mr/submit-job {:map @wordcount-map :mapper-fn mapper :reducer-fn r :tracker tracker})
                   res (.get fut 2 TimeUnit/SECONDS)]
               (log/infof "Result %s" res)
               res))))))

(deftest validation
  (testing "empty-source-map"
    (is (= {}
           (let [m (fn [k v] 1)
                 r (fn [v state] (reset! state {:val 1}))
                 tracker (mr/make-job-tracker @hz/hazelcast)]
             (-> (mr/submit-job {:map @validation-map :mapper-fn m :reducer-fn r :tracker tracker})
                 (.get 2 TimeUnit/SECONDS)))))
    )
  ;TODO extend this test when issue #2173 is fixed,exceptions are not propagated correctly
  ;https://github.com/hazelcast/hazelcast/issues/2173
  (testing "bad-mapper"
    (is (thrown? TimeoutException
                 (let [m (fn [k v] 1) ;bad mapper, should return collection
                       r (fn [v state] (reset! state {:val 1}))
                       tracker (mr/make-job-tracker @hz/hazelcast)]
                   (hz/put! @validation-map :k1 "clojure java")
                   (hz/put! @validation-map :k2 "java clojure")
                   (hz/put! @validation-map :k3 "lisp clojure")
                   (-> (mr/submit-job {:map @validation-map :mapper-fn m :reducer-fn r :tracker tracker})
                       (.get 2 TimeUnit/SECONDS))))))
  ;TODO #2173
  (testing "bad-reducer"
    (is (thrown? TimeoutException
                 (let [m (fn [k v] [[k 1]])
                       r (fn [v state] 1) ;bad reducer ,should set :val
                       tracker (mr/make-job-tracker @hz/hazelcast)]
                   (hz/put! @validation-map :k1 "clojure java")
                   (hz/put! @validation-map :k2 "java clojure")
                   (hz/put! @validation-map :k3 "lisp clojure")
                   (-> (mr/submit-job {:map @validation-map :mapper-fn m :reducer-fn r :tracker tracker})
                       (.get 2 TimeUnit/SECONDS)))))))