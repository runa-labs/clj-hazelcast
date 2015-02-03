(ns clj-hazelcast.test.distributed-test
  (:import (java.util.concurrent TimeoutException TimeUnit ExecutionException)
           (com.hazelcast.core Hazelcast)
           (com.hazelcast.config Config)
           (com.hazelcast.mapreduce Mapper Context)
           (cljhazelcast.remote RemoteMapper RemoteReducerFactory)
           (com.hazelcast.instance HazelcastInstanceFactory))
  (:use clojure.test)
  (:require [clj-hazelcast.core :as hz]
            [clj-hazelcast.mr :as mr]
            [clojure.tools.logging :as log]))

(def distributed-map (atom nil))

(def hz1 (atom nil))
(def hz2 (atom nil))

(def config
  (let [cfg (Config.)
        ;;cl (.getClassLoader (.getClass HazelcastInstanceFactory))
        cl (.getContextClassLoader (Thread/currentThread))]
    (.setEnabled (.. cfg getNetworkConfig getJoin getMulticastConfig) false)
    (.addMember (.setEnabled (.. cfg getNetworkConfig getJoin getTcpIpConfig) true) "127.0.0.1")
    (.addInterface (.setEnabled (.. cfg getNetworkConfig getInterfaces) true) "127.0.0.1")
    (.setClassLoader cfg cl)
    cfg))

(defmacro with-hz-binding
  [haz body]
  `(binding [clj-hazelcast.core/hazelcast ~haz]
     (~@body)))

(defn fixture [f]
  (reset! hz1 (hz/make-hazelcast config))
  (reset! hz2 (hz/make-hazelcast config))
  (reset! distributed-map (with-hz-binding hz1 (hz/get-map "distributed-map")))
  (f))


(use-fixtures :once fixture)


(mr/defmapper mapper1 [_ v]
              (let [words (re-seq #"\w+" v)]
                (partition 2 (interleave words (take (count words) (repeatedly (fn [] 1)))))))


(mr/defreducer reducer1 [_ v acc] (if (nil? acc)
                                    v
                                    (+ acc v)))


(deftest remote-reducer-test
  (testing "remote reducer java execution, created without the macro"
    (is (= 2 (let [rreducerf (RemoteReducerFactory. (pr-str '(fn [k v acc] (if (nil? acc)
                                                                             1
                                                                             (+ acc v)))))
                   rreducer (.newReducer rreducerf "key1")]
               (.reduce rreducer 1)
               (.reduce rreducer 1)
               (.finalizeReduce rreducer)
               )))
    ))

(deftest distributed-wordcount-test
  (testing "wordcount"
    (with-hz-binding hz1 (hz/put! @distributed-map :sentence1 "the quick brown fox jumps over the lazy dog"))
    (with-hz-binding hz2 (hz/put! @distributed-map :sentence2 "the fox and the hound"))
    (is (= {"the" 4, "over" 1, "quick" 1, "and" 1, "lazy" 1, "hound" 1, "jumps" 1, "brown" 1, "dog" 1, "fox" 2}
           (let [tracker (mr/make-job-tracker @hz1)
                 fut (mr/submit-job {:map @distributed-map :mapper-fn mapper1 :reducer-fn reducer1 :tracker tracker})
                 res (.get fut 2 TimeUnit/SECONDS)]
             (log/infof "Result %s" res)
             res)))))
