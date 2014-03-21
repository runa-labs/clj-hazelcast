(ns clj-hazelcast.mr
  (:import (com.hazelcast.mapreduce Mapper Reducer ReducerFactory KeyValueSource)
           (com.hazelcast.core ExecutionCallback))
  (:require [clj-hazelcast.core :as hazelcast]
            [clojure.tools.logging :as log]))


(defn- hmap
  "runs the function f over the content

  f is a function of two arguments, key and value.

  f must return a *pair* like
   [key1 value1]
  "
  [f]
  (proxy
      [Mapper] []
    (map [k v collector]
      (let [[k1 v1] (f k v)]
        (.emit collector k1 v1)
        ))))


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

(defn submit-job "returns the future object from HZ Mapreduce Api"
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
                      (log/infof "Calculation finished! :)"))
                    (onFailure [t]
                      (.printStackTrace t))))
    (.get fut)
    ))

