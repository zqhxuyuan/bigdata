package sparkly.core


object SparkDefaultConfiguration {

  val defaultBatchDurationMs = 1000L

  val defaultApplicationConfiguraion = Map (
    "spark.app.name" -> "sparkly",
    "spark.executor.memory" -> "512m",
    "spark.driver.memory" -> "512m",
    "spark.driver.maxResultSize" -> "1g",
    "spark.logConf" -> "false"
  )

  val defaultRuntimeConfiguration = Map (
    "spark.driver.extraJavaOptions" -> "",
    "spark.executor.extraJavaOptions" -> "",
    "spark.executor.logs.rolling.strategy" -> "time",
    "spark.executor.logs.rolling.time.interval" -> "daily",
    "spark.executor.logs.rolling.maxSize" -> "",
    "spark.executor.logs.rolling.maxRetainedFiles" -> ""
  )

  val defaultStreamingConfiguration = Map (
    "spark.streaming.blockInterval" -> "200ms",
    "spark.streaming.receiver.maxRate" -> "0",
    "spark.streaming.receiver.writeAheadLog.enable" -> "false",
    "spark.streaming.unpersist" -> "true"
  )

  val defaultShuffleConfiguration = Map (
    "spark.shuffle.consolidateFiles" -> "false",
    "spark.shuffle.spill" -> "true",
    "spark.shuffle.spill.compress" -> "true",
    "spark.shuffle.memoryFraction" -> "0.2",
    "spark.shuffle.compress" -> "true",
    "spark.shuffle.file.buffer" -> "32k",
    "spark.reducer.maxSizeInFlight" -> "48k",
    "spark.shuffle.manager" -> "sort",
    "spark.shuffle.sort.bypassMergeThreshold" -> "200",
    "spark.shuffle.blockTransferService" -> "netty"
  )

  val defaultCompressionConfiguration = Map (
    "spark.broadcast.compress" -> "true",
    "spark.rdd.compress" -> "false",
    "spark.io.compression.codec" -> "snappy",
    "spark.io.compression.snappy.blockSize" -> "32k",
    "spark.io.compression.lz4.blockSize" -> "32k",
    "spark.serializer.objectStreamReset" -> "100",
    "spark.kryo.referenceTracking" -> "true",
    "spark.kryo.registrationRequired" -> "false",
    "spark.kryoserializer.buffer" -> "64k",
    "spark.kryoserializer.buffer.max" -> "64m"
  )

  val defaultExecutionConfiguration = Map (
    "spark.default.parallelism" -> "8",
    "spark.broadcast.factory" -> "org.apache.spark.broadcast.TorrentBroadcastFactory",
    "spark.broadcast.blockSize" -> "4m",
    "spark.files.overwrite" -> "false",
    "spark.files.fetchTimeout" -> "60s",
    "spark.storage.memoryFraction" -> "0.6",
    "spark.storage.unrollFraction" -> "0.2",
    "spark.storage.memoryMapThreshold" -> "2m",
    "spark.externalBlockStore.url" -> "tachyon://localhost:19998",
    "spark.cleaner.ttl" -> "3600",
    "spark.hadoop.validateOutputSpecs" -> "true",
    "spark.hadoop.cloneConf" -> "false",
    "spark.executor.heartbeatInterval" -> "10s"
  )

  val defaultSchedulingConfiguration = Map (
    "spark.task.cpus" -> "1",
    "spark.task.maxFailures" -> "4",
    "spark.scheduler.mode" -> "FIFO",
    "spark.cores.max" -> "",
    "spark.speculation" -> "false",
    "spark.speculation.interval" -> "100ms",
    "spark.speculation.quantile" -> "0.75",
    "spark.speculation.multiplier" -> "1.5",
    "spark.locality.wait" -> "3s",
    "spark.locality.wait.process" -> "3s",
    "spark.locality.wait.node" -> "3s",
    "spark.locality.wait.rack" -> "3s",
    "spark.scheduler.revive.interval" -> "1s",
    "spark.scheduler.minRegisteredResourcesRatio" -> "0.0",
    "spark.scheduler.maxRegisteredResourcesWaitingTime" -> "30s",
    "spark.localExecution.enabled" -> "false"
  )

  val defaultAllocationConfiguration = Map (
    "spark.dynamicAllocation.enabled" -> "false",
    "spark.dynamicAllocation.minExecutors" -> "",
    "spark.dynamicAllocation.maxExecutors" -> "",
    "spark.dynamicAllocation.schedulerBacklogTimeout" -> "1s",
    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout" -> "1s",
    "spark.dynamicAllocation.executorIdleTimeout" -> "60s"
  )

  val defaultSettings = Map[String, Map[String, String]](
    "Application Properties" -> defaultApplicationConfiguraion,
    "Runtime Environment" -> defaultRuntimeConfiguration,
    "Streaming" -> defaultStreamingConfiguration,
    "Shuffle Behavior" -> defaultShuffleConfiguration,
    "Compression and Serialization" -> defaultCompressionConfiguration,
    "Execution Behavior" -> defaultExecutionConfiguration,
    "Scheduling" -> defaultSchedulingConfiguration,
    "Dynamic allocation" -> defaultAllocationConfiguration
  )

}