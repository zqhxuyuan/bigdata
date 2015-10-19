(ns hambo.conf)

(def ^:const HAMBO-CONFIGURATION-KEY "hambo.config")
(def ^:const HAMBO-HOSTS "hambo.hosts")
(def ^:const HAMBO-MAX-BATCH-SIZE "hambo.max.batch.size")
(def ^:const HAMBO-RECONNECTION-POLICY "hambo.reconnection.policy")
(def ^:const HAMBO-LOAD-BALANCING-POLICY "hambo.load.balancing.policy")
(def ^:const HAMBO-RETRY-POLICY "hambo.retry.policy")
(def ^:const HAMBO-CONSISTENCY-LEVEL "hambo.consistency.level")

(defn configuration [hosts max-batch-size]
  {HAMBO-CONFIGURATION-KEY
   {HAMBO-HOSTS hosts
    HAMBO-MAX-BATCH-SIZE max-batch-size}})