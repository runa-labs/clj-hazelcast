(ns clj-hazelcast.core
  (:require
   [clj-kryo.core :as kryo])
  (:import
   (com.hazelcast.core Hazelcast HazelcastInstance EntryListener ItemListener
                       IMap IList EntryEvent)
   (com.hazelcast.config XmlConfigBuilder Config TcpIpConfig)
   (java.util Collection Set List Map HashSet Queue)
   java.util.concurrent.locks.Lock
   [java.util.concurrent BlockingQueue TimeUnit]))

(def ^:dynamic hazelcast (atom nil))

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
  (Hazelcast/newHazelcastInstance
   (if (instance? com.hazelcast.config.Config opts)
     opts
     (make-hazelcast-config opts))))

(defn init [& [opts]]
  (when-not @hazelcast
    (reset! hazelcast (make-hazelcast opts))))

(defn with-instance [^HazelcastInstance instance]
  (reset! hazelcast instance))

(defn shutdown []
  (let [instance ^HazelcastInstance @hazelcast]
    (reset! hazelcast nil)
    (when instance
      (.shutdown ^HazelcastInstance instance))))

;; (init)

(defn do-with-lock [lockable thunk]
  (let [^Lock lock (.getLock ^HazelcastInstance @hazelcast lockable)]
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

(defn put-ttl! [^IMap m key value ttl-seconds]
  (.put m key (kryo/wrap-kryo-serializable value) ttl-seconds TimeUnit/SECONDS))

(defn put-all! [^IMap dest ^Map src]
  (.putAll dest src))

(defn clear! [m] (.clear ^IMap m))

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
    (.addEntryListener m listener true)))

(defn remove-entry-listener! [^Map m ^String id]
  (.removeEntryListener ^IMap m id))

(defn ^List get-list [name]
  (.getList ^HazelcastInstance @hazelcast name))

(defn ^Set get-set [name]
  (.getSet ^HazelcastInstance @hazelcast name))

(defn add! [^Collection list-or-set-or-queue item]
  (.add list-or-set-or-queue (kryo/wrap-kryo-serializable item)))

(defn add-all! [^Collection list-or-set-or-queue items]
  (.addAll list-or-set-or-queue items))

(defn add-item-listener! [^IList list listener-fn]
  (let [listener (proxy [ItemListener] []
                   (itemAdded [item]
                     (listener-fn :add item))
                   (itemRemoved [item]
                     (listener-fn :remove item)))]
    (.addItemListener list listener true)
    listener))


;queue related
(defn ^BlockingQueue get-queue [name]
  "returns a distributed blocking queue instance based on Hazelcast"
  (.getQueue ^HazelcastInstance @hazelcast name))

(defn take! [^BlockingQueue queue]
  (.take queue))
