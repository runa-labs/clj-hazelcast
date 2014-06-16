(ns clj-hazelcast.test.mr-test
  (:import (java.util.concurrent TimeoutException TimeUnit ExecutionException))
  (:use clojure.test)
  (:require [clj-hazelcast.core :as hz]
            [clj-hazelcast.mr :as mr]
            [clojure.tools.logging :as log]))


(def mr-test-map (atom nil))
(def wordcount-map (atom nil))
(def validation-map (atom nil))

(defn fixture [f]
  (hz/init)
  (reset! mr-test-map (hz/get-map "caster.cluster-tests.mr-test-map"))
  (reset! wordcount-map (hz/get-map "caster.cluster-tests.wordcount-map"))
  (reset! validation-map (hz/get-map "caster.cluster-tests.validation-map"))
  (f))

(use-fixtures :once fixture)

(defn split-words
  "split text into a list of words"
  [text]
  (re-seq #"\w+" text))

(defn calculate-frequencies
  "convert list of words to a word-frequency hash"
  [words]
  (reduce (fn [words word] (assoc words word (inc (get words word 0))))
          {}
          words))

(deftest mapreduce-test
  (testing "key-count"
    (is (= {:k1 1 :k2 1 :k5 1 :k3 1 :k4 1}
           (let [m (fn [k _] [[k 1]])
                 r (fn [k v acc] (inc acc))]
             (hz/put! @mr-test-map :k1 "v1")
             (hz/put! @mr-test-map :k2 "v2")
             (hz/put! @mr-test-map :k3 "v3")
             (hz/put! @mr-test-map :k4 "v4")
             (hz/put! @mr-test-map :k5 "v5")
             (let [tracker (mr/make-job-tracker @hz/hazelcast)
                   fut (mr/submit-job {:map @mr-test-map :mapper-fn m :reducer-fn r :reducer-acc 0 :tracker tracker})
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
           (let [r (fn [k v acc] (inc acc))]
             (hz/put! @wordcount-map :k1 "clojure java")
             (hz/put! @wordcount-map :k2 "java clojure")
             (hz/put! @wordcount-map :k3 "lisp clojure")
             (let [tracker (mr/make-job-tracker @hz/hazelcast)
                   fut (mr/submit-job {:map @wordcount-map :mapper-fn mapper :reducer-fn r :reducer-acc 0 :tracker tracker})
                   res (.get fut 2 TimeUnit/SECONDS)]
               (log/infof "Result %s" res)
               res))))))

(deftest validation
  (testing "empty-source-map"
    (is (= {}
           (let [m (fn [_ _] 1)
                 r (fn [k v acc] (inc acc))
                 tracker (mr/make-job-tracker @hz/hazelcast)]
             (-> (mr/submit-job {:map @validation-map :mapper-fn m :reducer-fn r :reducer-acc 0 :tracker tracker})
                 (.get 2 TimeUnit/SECONDS)))))
    )
  (testing "bad-mapper"
    (is (thrown? ExecutionException
                 (let [m (fn [_ _] 1)                       ;bad mapper, should return a collection
                       r (fn [k v acc] (inc acc))
                       tracker (mr/make-job-tracker @hz/hazelcast)]
                   (hz/put! @validation-map :k1 "bad mapper")
                   (hz/put! @validation-map :k2 "mapper bad")
                   (hz/put! @validation-map :k3 "mapper mapper")
                   (-> (mr/submit-job {:map @validation-map :mapper-fn m :reducer-fn r :reducer-acc 0 :tracker tracker})
                       (.get 2 TimeUnit/SECONDS))))))
  (testing "bad-reducer"
    (is (thrown? ExecutionException
                 (let [m (fn [k _] [[k 1]])
                       r (fn [k v acc] nil)                 ;bad reducer ,should return new accumulator
                       tracker (mr/make-job-tracker @hz/hazelcast)]
                   (hz/put! @validation-map :k1 "clojure java")
                   (hz/put! @validation-map :k2 "java clojure")
                   (hz/put! @validation-map :k3 "lisp clojure")
                   (-> (mr/submit-job {:map @validation-map :mapper-fn m :reducer-fn r :reducer-acc 0 :tracker tracker})
                       (.get 2 TimeUnit/SECONDS))))))
  )