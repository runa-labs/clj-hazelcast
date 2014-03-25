(require '[clj-hazelcast.core :as hazelcast])
(require '[clj-hazelcast.mr :as mr] :reload)
(require '[clojure.tools.logging :as log])

(do
  (def test-map (atom nil))

  (hazelcast/init)
  (reset! test-map (hazelcast/get-map "clj-hazelcast.cluster-tests.test-map"))

  (hazelcast/put! @test-map :baz "foo")
  (hazelcast/put! @test-map :asd "bar")

  (eval @test-map)

  ;desired mapper sample
  (defn map1 [key value]
    (println key value)
    [[key 1]])

  ;desired reducer sample
  (defn reducer1 [v state]
    (let [old @state]
      (log/infof "Reducing %s" old)
      (if-not (nil? (:val old))
        (reset! state (assoc old :val (inc (:val old))))
        (reset! state (assoc old :val 1))
        )
      (log/infof "Reduced %s" @state)
      ))

  ;desired combiner sample
  (defn combiner1 [v state]
    (let [old @state]
      (log/infof "Combining %s" old)
      (if-not (nil? (:val old))
        (reset! state (assoc old :val (inc (:val old))))
        (reset! state (assoc old :val 1))
        )
      (log/infof "Combined %s" @state)
      ))

  ;collator
  (defn collator1 [seq]
    (reduce + (map #(val %) seq)))

  (eval @hazelcast/hazelcast)
  (eval @test-map)
  )

(try
  (let [tracker (mr/make-job-tracker @hazelcast/hazelcast)
        res (mr/submit-job {:map @test-map
                            :mapper-fn map1
                            :reducer-fn reducer1
                            :tracker tracker
                            ;:combiner-fn combiner1
                            ;:collator-fn collator1
                            })]
    (log/infof "Result %s" res)
    res)
  (catch Exception e
    (.printStackTrace e)))
