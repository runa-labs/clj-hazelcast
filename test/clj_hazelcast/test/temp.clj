(ns temp.clj
  (:import (com.hazelcast.mapreduce ReducerFactory)))

(require '[clj-hazelcast.core :as hazelcast])

(def test-map (atom nil))

(hazelcast/init)
(reset! test-map (hazelcast/get-map "clj-hazelcast.cluster-tests.test-map"))

(hazelcast/put! @test-map :baz "foobar")

@test-map

(let [events (atom [])
      listener-fn (fn [& event]
                    (swap! events conj event))
      listener (hazelcast/add-entry-listener! @test-map listener-fn)
      result (do
               (hazelcast/put! @test-map :baz "foobar")
               (hazelcast/put! @test-map :foo "bizbang")
               (Thread/sleep 5)
               (count @events))]
  (hazelcast/remove-entry-listener! @test-map listener)
  result)



(def jobtracker (.getJobTracker @hazelcast/hazelcast "default"))




;desired mapper sample
(defn m1 [key value]
  (println key value)
  [1 2])

;desired reducer sample
(defn r1 [key values]
  (println (class values))
  (count values))


(import '(com.hazelcast.mapreduce Mapper))
(defn hmap
  "runs the function f over the content

  f is a function of two arguments, key and value.

  f must return a *sequence* of *pairs* like
    [[key1 value1] [key2 value2] ...]
  "
  [f]
  (proxy
      [Mapper] []
    (map [k v collector]
      (let [[k1 v1] (f k v)]
        (.emit collector k1 v1)
        ))))

(import '(com.hazelcast.mapreduce Reducer))
(import '(com.hazelcast.mapreduce ReducerFactory))

(defn hreduce [f]
  (proxy
      [Reducer] []
    (reduce [k vals]
      (f k (iterator-seq vals)))
    (finalizeReduce []
      1))
  )

(defn hreducefactory
  "runs the function f over the content

  f is a function of two arguments, key and value.

  f must return a *sequence* of *pairs* like
    [[key1 value1] [key2 value2] ...]
  "
  [f]
  (proxy [ReducerFactory] []
    (newReducer [key]
      (hreduce f))
    ))



(import '(com.hazelcast.mapreduce.impl MapKeyValueSource))
(def src (MapKeyValueSource/fromMap @test-map))

(def job (.newJob jobtracker src))
(def j1 (doto job
          (.mapper (hmap m1))
          (.combiner nil)
          (.reducer (hreducefactory r1))
          ))

(.submit j1)
