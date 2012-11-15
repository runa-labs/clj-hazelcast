(defproject com.runa/clj-hazelcast "1.0.1"
  :description "Clojure library for the Hazelcast p2p cluster"
  :plugins [[s3-wagon-private "1.1.2"]
            [lein-swank "1.4.4"]]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.runa/clj-kryo "1.2.0"]
                 [com.hazelcast/hazelcast "2.4"]]
  :repositories {"releases" {:url "s3p://runa-maven/releases/"}})
