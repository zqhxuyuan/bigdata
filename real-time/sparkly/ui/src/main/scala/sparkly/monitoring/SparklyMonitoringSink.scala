package org.apache.spark.metrics.sink

import com.codahale.metrics._
import java.util.concurrent.TimeUnit
import sparkly.config.SparklyConfig._
import java.util.{Properties, SortedMap}
import scala.collection.JavaConversions._
import sparkly.monitoring.MonitoringDataSender
import sparkly.utils.NumberUtils
import sparkly.core.MonitoringData

class SparklyMonitoringSink(val property: Properties, val registry: MetricRegistry, securityMgr: org.apache.spark.SecurityManager) extends Sink {

  val clusterId = "local"
  val reporter = new SparklyMonitoringReporter(clusterId, registry)

  override def start(): Unit = {
    reporter.start(1, TimeUnit.SECONDS)
  }

  override def stop(): Unit = {
    reporter.stop()
  }

  override def report(): Unit = {
    reporter.report()
  }
}

class SparklyMonitoringReporter (
    clusterId: String,
    registry: MetricRegistry,
    name: String = "monitoring-reporter",
    filter: MetricFilter = MetricFilter.ALL,
    rateUnit: TimeUnit = TimeUnit.SECONDS,
    durationUnit: TimeUnit = TimeUnit.MILLISECONDS)
  extends ScheduledReporter(
    registry: MetricRegistry,
    name: String,
    filter: MetricFilter,
    rateUnit: TimeUnit,
    durationUnit: TimeUnit) {

  val dataSender = new MonitoringDataSender(HOSTNAME, WEB_PORT)

  override def report(
     gauges: SortedMap[String, Gauge[_]],
     counters: SortedMap[String, Counter],
     histograms: SortedMap[String, Histogram],
     meters: SortedMap[String, Meter],
     timers: SortedMap[String, Timer]): Unit = {

    // TODO : We may have multiple gauges! 1 per worker ??!?

    gauges
      .filter(_._1 contains "monitoring.")
      .foreach{case (name, gauge) =>
        val Array(sourceId, monitoringId) = name.split("-monitoring.")
        gauge.getValue.asInstanceOf[Option[MonitoringData[_]]] match {
          case Some(data) => dataSender.send(data, clusterId, monitoringId)
          case None => // do nothing
        }
      }

    dataSender.send(jvmData(gauges), clusterId, "pipeline-memory")
    dataSender.send(latencyData(gauges), clusterId, "pipeline-latency")
  }

  private def jvmData(gauges: SortedMap[String, Gauge[_]]): MonitoringData[Map[String, Double]] = {
    val memoryUsed = gauges.filter(_._1 endsWith "jvm.total.used").map(_._2).map(gauge => gauge.getValue.toString.toLong).foldLeft(0L)(_ + _)
    val memoryCommitted = gauges.filter(_._1 endsWith "jvm.total.committed").map(_._2).map(gauge => gauge.getValue.toString.toLong).foldLeft(0L)(_ + _)
    val maxMemory = gauges.filter(_._1 endsWith "jvm.total.max").map(_._2).map(gauge => gauge.getValue.toString.toLong).foldLeft(0L)(_ + _)

    MonitoringData(System.currentTimeMillis, Map(
      "Memory used" -> NumberUtils.toGB(memoryUsed, 2),
      "Memory committed" -> NumberUtils.toGB(memoryCommitted, 2),
      "Max memory" -> NumberUtils.toGB(maxMemory, 2)
    ))
  }

  private def latencyData(gauges: SortedMap[String, Gauge[_]]): MonitoringData[Map[String, Double]] = {
    val processingDelay = gauges.find(_._1 endsWith "StreamingMetrics.streaming.lastCompletedBatch_processingDelay").map(_._2).map(gauge => gauge.getValue.toString.toDouble).getOrElse(0.0)
    val schedulingDelay = gauges.find(_._1 endsWith "StreamingMetrics.streaming.lastCompletedBatch_schedulingDelay").map(_._2).map(gauge => gauge.getValue.toString.toDouble).getOrElse(0.0)
    val totalDelay = gauges.find(_._1 endsWith "StreamingMetrics.streaming.lastCompletedBatch_totalDelay").map(_._2).map(gauge => gauge.getValue.toString.toDouble).getOrElse(0.0)

    MonitoringData(System.currentTimeMillis, Map(
      "Processing delay" -> processingDelay,
      "Scheduling delay" -> schedulingDelay,
      "Total delay" -> totalDelay
    ))
  }
}
