(ns clj-hazelcast.query
  (:import (java.util Set)
           (com.hazelcast.core IMap)
           (com.hazelcast.query Predicate)
           (com.hazelcast.query.impl QueryEntry)
           (cljhazelcast.remote RemotePredicate)))


(defn ^Set values
  [^IMap m ^Predicate predicate]
  "Executes the predicate on the given map and returns the results"
  (.values m predicate))

(defmacro defpredicate
  "Execute the given predicate over the dataset, hz will return the matching ones
   A predicate function is a function with 2 parameters, a key and a value and returns a boolean value
  "
  [fname args & body]
  `(let [instance# (RemotePredicate.
                     (pr-str '(fn ~args ~@body)))]
     (def ~fname instance#)))

