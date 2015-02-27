(defproject org.clojars.runa/clj-hazelcast "1.2.1"
  :description "Clojure library for the Hazelcast p2p cluster"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojars.runa/clj-kryo "1.5.0"]
                 [com.hazelcast/hazelcast "3.4"]]
  :profiles {:provided {:dependencies [[org.clojure/tools.logging "0.2.6"]]}}
  :global-vars {*warn-on-reflection* true}
  :min-lein-version "2.0.0" ;provided deps
  :java-source-paths ["src-java"])
