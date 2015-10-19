(ns hambo.trident.cql
  (:require [marceline.storm.trident :as t]
            [clojurewerkz.cassaforte.client :as cc]
            [hambo.trident.conf :as conf]
            [clojure.string :as s])
  (:import [storm.trident.state.map
            NonTransactionalMap
            TransactionalMap
            OpaqueMap
            CachedMap
            SnapshottableMap]
           [calculon.storm.trident.state LRUCachedMap]))

(defn mk-map-state [state-type backing-map]
  (case state-type
    :transactional (TransactionalMap/build backing-map)
    :nontransactional (NonTransactionalMap/build backing-map)
    :opaque (OpaqueMap/build backing-map)))

(t/defstatefactory
  cql-state-factory
  {:params [keyspace table backing-hof state-type serializer]}
  [conf metrics partition-index num-partitions]
  (let [hambo-config (get conf conf/HAMBO-CONFIGURATION-KEY)
        conn (cc/connect (s/split (get hambo-config conf/HAMBO-HOSTS) #",") {:keyspace keyspace})
        backing-map (backing-hof conn table serializer)
        cache-size 20000
        cached-map (CachedMap. backing-map cache-size)]
    (mk-map-state state-type cached-map)))

(t/defstatefactory
  instrumented-cql-state-factory
  {:params [keyspace table backing-hof cache-size state-type prefix-name bucket-size]}
  [conf metrics partition-index num-partitions]
  (let [hambo-config (get conf conf/HAMBO-CONFIGURATION-KEY)
        conn (cc/connect (s/split (get hambo-config conf/HAMBO-HOSTS) #",") {:keyspace keyspace})
        instrumented-map (backing-hof conn table)
        _ (.registerMetrics instrumented-map conf metrics prefix-name (int bucket-size))
        cached-map (LRUCachedMap. instrumented-map cache-size)]
    (mk-map-state state-type cached-map)))