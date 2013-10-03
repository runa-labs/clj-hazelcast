(defproject com.runa/clj-hazelcast "1.0.1"
  :description "Clojure library for the Hazelcast p2p cluster"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojars.runa/clj-kryo "1.3.0"]
                 [com.hazelcast/hazelcast "2.4"]]
  :repositories {"releases" {:url "s3p://runa-maven/releases/"}})
