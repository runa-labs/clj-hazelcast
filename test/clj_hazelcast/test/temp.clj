(require '[clj-hazelcast.core :as hazelcast])

(def test-map (atom nil))

(hazelcast/init)
(reset! test-map (hazelcast/get-map "clj-hazelcast.cluster-tests.test-map"))

(hazelcast/put! @test-map :baz "foobar")
(hazelcast/put! @test-map :asd "asd")

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
(defn r1 [v state]
  (println v)
  (println @state)
  (let [val {:val @state}]
    (reset! @state (assoc @state :val (inc val)))
    )
  )



(def job (.newJob jobtracker src))
(def j1 (.reducer (.mapper job (hmap m1)) (hreducefactory r1)))

(def fut (.submit j1))
(->> (.getJobId fut)
     (.getTrackableJob jobtracker)
     (.getJobProcessInformation)
     (.getProcessedRecords))

(import '(com.hazelcast.core ExecutionCallback))
