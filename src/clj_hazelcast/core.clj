(ns clj-hazelcast.core
  (:require
   [clj-kryo.core :as kryo])
  (:import
   [com.hazelcast.core
    Hazelcast HazelcastInstance EntryListener ItemListener
    IMap IList EntryEvent]
   [com.hazelcast.config XmlConfigBuilder TcpIpConfig]
   [java.util Set List Map HashSet Queue]
   java.util.concurrent.locks.Lock))

(def ^:private hazelcast (atom nil))

(defn make-hazelcast-config [opts]
  (let [config (.build (XmlConfigBuilder.))]
    (when-let [peers (:peers opts)]
      (let [tcp-config (-> config
                           (.getNetworkConfig)
                           (.getJoin)
                           (.getTcpIpConfig))]
        (doseq [peer peers]
          (.addMember tcp-config peer))))
    config))

(defn make-hazelcast [opts]
  (Hazelcast/newHazelcastInstance (make-hazelcast-config opts)))

(defn init [& [opts]]
  (when-not @hazelcast
    (reset! hazelcast (make-hazelcast opts))))

;; (init)

(defn do-with-lock [lockable thunk]
  (let [^Lock lock (.getLock @hazelcast lockable)]
    (.lock lock)
    (try
      (thunk)
      (finally
        (.unlock lock)))))

(defmacro with-lock [lockable & body]
  `(do-with-lock ~lockable (fn [] ~@body)))

(defn ^Map get-map [name]
  (.getMap ^HazelcastInstance @hazelcast name))

(defn put! [^IMap m key value]
  (.put m key (kryo/wrap-kryo-serializable value)))

(defn put-all! [^IMap dest ^Map src]
  (.putAll dest src))

(defn clear! [m] (.clear m))

(defn add-entry-listener! [^IMap m listener-fn]
  (let [listener (proxy [EntryListener] []
                   (entryAdded [^EntryEvent event]
                     (listener-fn :add (.getKey event) (.getValue event)))
                   (entryRemoved [^EntryEvent event]
                     (listener-fn :remove (.getKey event) (.getValue event)))
                   (entryUpdated [^EntryEvent event]
                     (listener-fn :update (.getKey event) (.getValue event)))
                   (entryEvicted [^EntryEvent event]
                     (listener-fn :evict (.getKey event) (.getValue event))))]
    (.addEntryListener m listener true)
    listener))

(defn remove-entry-listener! [^Map m listener]
  (.removeEntryListener ^IMap m listener))

(defn ^List get-list [name]
  (.getList ^HazelcastInstance @hazelcast name))

(defn ^Set get-set [name]
  (.getSet ^HazelcastInstance @hazelcast name))

(defn add! [list-or-set item]
  (.add list-or-set (kryo/wrap-kryo-serializable item)))

(defn add-all! [list-or-set items]
  (.addAll list-or-set items))

(defn add-item-listener! [^IList list listener-fn]
  (let [listener (proxy [ItemListener] []
                   (itemAdded [item]
                     (listener-fn :add item))
                   (itemRemoved [item]
                     (listener-fn :remove item)))]
    (.addItemListener list listener true)
    listener))
