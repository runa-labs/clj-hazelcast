(ns clj-hazelcast.mr
  (:import (com.hazelcast.mapreduce Mapper Reducer ReducerFactory KeyValueSource)
           (com.hazelcast.core ExecutionCallback))
  (:require [clj-hazelcast.core :as hazelcast]
            [clojure.tools.logging :as log]))

(defn emit-mapped-pairs [pairs collector]
  (doall
    (map #(do
           (log/debugf "emitting pair: %s" %1)
           (let [emit-key (first %1)
                 emit-value (second %1)]
             (log/debugf "emitting key: %s emit vlaue: %s " emit-key emit-value)
             (.emit collector emit-key emit-value)
             ))
         pairs))
  )

(defn- hmap
  "runs the function f over the content

  f is a function of two arguments, key and value.

  f must return a sequence of *pairs* like
   [[key1 value1] [key2 value2] ...]
  "
  [f]
  (proxy
      [Mapper] []
    (map [k v collector]
      (let [pair-seq (f k v)]
        (emit-mapped-pairs pair-seq collector)
        )
      )))


(defn- hreducefactory
  "runs the reducer function rf over the content

  rf is a function of two arguments, value and an atom containing the state

  state does contain the key:

  {:key key
   :val val}

  rf should set the val eventually
  "
  [f]
  (proxy [ReducerFactory] []
    (newReducer [key]
      (let [state (atom nil)]
        (proxy
            [Reducer] []
          (beginReduce [k]
            (do
              (log/debugf "beginReduce with %s" k)
              (reset! state {:key k}))
            )
          (reduce [v]
            (do
              (log/debugf "reduce with %s" v)
              (f v state))
            )
          (finalizeReduce []
            (do
              (log/debugf "finalizeReduce")
              (:val @state))))))))


(defn make-job-tracker [hz-instance]
  (.getJobTracker hz-instance "default")
  )

(defn submit-job "returns the future object from Hazelcast Mapreduce Api"
  [map mapper-fn reducer-fn tracker]
  (let [src (KeyValueSource/fromMap map)
        mapper (hmap mapper-fn)
        reducer (hreducefactory reducer-fn)
        fut (-> (.newJob tracker src)
                (.mapper mapper)
                (.reducer reducer)
                (.submit))]
    (.andThen fut (proxy [ExecutionCallback] []
                    (onResponse [res]
                      (log/infof "Calculation finished..."))
                    (onFailure [t]
                      (.printStackTrace t))))
    (.get fut)
    ))

