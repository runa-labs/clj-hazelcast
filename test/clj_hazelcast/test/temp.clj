(require '[clj-hazelcast.core :as hazelcast])
(require '[clj-hazelcast.mr :as mr])
(require '[clojure.tools.logging :as log])

(def test-map (atom nil))

(hazelcast/init)
(reset! test-map (hazelcast/get-map "clj-hazelcast.cluster-tests.test-map"))

(hazelcast/put! @test-map :baz "foobar")
(hazelcast/put! @test-map :asd "asd")

(eval @test-map)

;desired mapper sample
(defn m1 [key value]
  (println key value)
  [key 1])

;desired reducer sample
(defn r1 [v state]
  (println v)
  (println @state)
  (let [val {:val @state}]
    (reset! @state (assoc @state :val (inc val)))
    )
  )


(let [tracker (mr/make-job-tracker @hazelcast/hazelcast)
      res (mr/submit-job @test-map m1 r1 tracker)]
  (log/infof "Result %s" res)
  res)
