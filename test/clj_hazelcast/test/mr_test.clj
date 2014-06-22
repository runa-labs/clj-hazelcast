(ns clj-hazelcast.test.mr-test
  (:import (java.util.concurrent TimeoutException TimeUnit ExecutionException))
  (:use clojure.test)
  (:require [clj-hazelcast.core :as hz]
            [clj-hazelcast.mr :as mr]
            [clojure.tools.logging :as log]))


(def mr-test-map (atom nil))
(def wordcount-map (atom nil))
(def validation-map (atom nil))
(def combiner-map (atom nil))

(defn fixture [f]
  (hz/init)
  (reset! mr-test-map (hz/get-map "clj-hazelcast.cluster-tests.mr-test-map"))
  (reset! wordcount-map (hz/get-map "clj-hazelcast.cluster-tests.wordcount-map"))
  (reset! combiner-map (hz/get-map "clj-hazelcast.cluster-tests.combiner-map"))
  (reset! validation-map (hz/get-map "clj-hazelcast.cluster-tests.validation-map"))
  (f))

(use-fixtures :once fixture)

(mr/defmapper m1 [k _] [[k 1]])
(mr/defreducer r1 [k v acc] (if (nil? acc) v (+ acc v)))

(deftest simple-test
  (testing "key-count"
    (is (= {:k1 1 :k2 1 :k5 1 :k3 1 :k4 1}
           (do
             (hz/put! @mr-test-map :k1 "v1")
             (hz/put! @mr-test-map :k2 "v2")
             (hz/put! @mr-test-map :k3 "v3")
             (hz/put! @mr-test-map :k4 "v4")
             (hz/put! @mr-test-map :k5 "v5")
             (let [tracker (mr/make-job-tracker @hz/hazelcast)
                   fut (mr/submit-job {:map @mr-test-map :mapper-fn m1 :reducer-fn r1 :tracker tracker})
                   res (.get fut 2 TimeUnit/SECONDS)]
               (log/infof "Result %s" res)
               res))))))



(mr/defmapper mapper
              [k v]
              (let [words (re-seq #"\w+" v)]
                (partition 2 (interleave words (take (count words) (repeatedly (fn [] 1)))))))


(deftest wordcount-test
  (testing "mapper"
    ;todo ugly - make it a concrete function
    (is (= '(("foo" 1) ("bar" 1)) ((eval (read-string (.getFun mapper))) "some-key" "foo bar"))))
  (testing "wordcount"
    (is (= {"clojure" 3 "java" 2 "lisp" 1}
           (do
             (hz/put! @wordcount-map :k1 "clojure java")
             (hz/put! @wordcount-map :k2 "java clojure")
             (hz/put! @wordcount-map :k3 "lisp clojure")
             (let [tracker (mr/make-job-tracker @hz/hazelcast)
                   fut (mr/submit-job {:map @wordcount-map :mapper-fn mapper :reducer-fn r1 :tracker tracker})
                   res (.get fut 2 TimeUnit/SECONDS)]
               (log/infof "Result %s" res)
               res))))))


(mr/defcombiner c1 [k v acc] (if (nil? acc) 1 (+ acc v)))

(deftest wordcount-test-with-combiners
  (testing "combiners"
    (is (= {"the" 4, "over" 1, "quick" 1, "and" 1, "lazy" 1, "hound" 1, "jumps" 1, "brown" 1, "dog" 1, "fox" 2}
           (do
             (hz/put! @combiner-map :sentence1 "the quick brown fox jumps over the lazy dog")
             (hz/put! @combiner-map :sentence2 "the fox and the hound")
             (let [tracker (mr/make-job-tracker @hz/hazelcast)
                   fut (mr/submit-job {:map @combiner-map :mapper-fn mapper :combiner-fn c1 :reducer-fn r1 :tracker tracker})
                   res (.get fut 2 TimeUnit/SECONDS)]
               (log/infof "Result %s" res)
               res))))
    )
  )


(mr/defmapper bad-mapper [_ _] 1)
;bad mapper, should return a collection
(mr/defreducer bad-reducer [k v acc] nil)
;bad reducer ,should return a new accumulator


(deftest validation
  (testing "when input is empty"
    (is (= {}
           (let [tracker (mr/make-job-tracker @hz/hazelcast)]
             (-> (mr/submit-job {:map @validation-map :mapper-fn bad-mapper :reducer-fn r1 :tracker tracker})
                 (.get 2 TimeUnit/SECONDS)))))
    )
  (testing "bad-mapper"
    (is (thrown? ExecutionException
                 (let [tracker (mr/make-job-tracker @hz/hazelcast)]
                   (hz/put! @validation-map :k1 "bad mapper")
                   (hz/put! @validation-map :k2 "mapper bad")
                   (hz/put! @validation-map :k3 "mapper mapper")
                   (-> (mr/submit-job {:map @validation-map :mapper-fn bad-mapper :reducer-fn r1 :tracker tracker})
                       (.get 2 TimeUnit/SECONDS))))))
  (testing "bad-reducer"
    (is (thrown? ExecutionException
                 (let [tracker (mr/make-job-tracker @hz/hazelcast)]
                   (hz/put! @validation-map :k1 "clojure java")
                   (hz/put! @validation-map :k2 "java clojure")
                   (hz/put! @validation-map :k3 "lisp clojure")
                   (-> (mr/submit-job {:map @validation-map :mapper-fn m1 :reducer-fn bad-reducer :tracker tracker})
                       (.get 2 TimeUnit/SECONDS))))))
  )

(mr/defcollator col-fn [seq] (reduce + (vals seq)))

(deftest collator
  (testing "collator functionality - get total count")
  (is (= 15
         (do
           (hz/put! @combiner-map :sentence1 "the quick brown fox jumps over the lazy dog")
           (hz/put! @combiner-map :sentence2 "the fox and the hound the")
           (let [tracker (mr/make-job-tracker @hz/hazelcast)
                 fut (mr/submit-job {:map @combiner-map :mapper-fn mapper :reducer-fn r1 :collator-fn col-fn :tracker tracker})
                 res (.get fut 2 TimeUnit/SECONDS)]
             (log/infof "Result %s" res)
             res))))
  )