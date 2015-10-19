package org.apache.spark.metrics.sink

import com.codahale.metrics._
import java.util.concurrent.TimeUnit
import java.util.{Properties, SortedMap}
import scala.collection.JavaConversions._
import sparkly.core.MonitoringData

class SparklyMonitoringTestingSink(val property: Properties, val registry: MetricRegistry, securityMgr: org.apache.spark.SecurityManager) extends Sink {

  val reporter = new SparklyMonitoringTestingReporter(registry)

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

class SparklyMonitoringTestingReporter (
    registry: MetricRegistry,
    name: String = "monitoring-testing-reporter",
    filter: MetricFilter = MetricFilter.ALL,
    rateUnit: TimeUnit = TimeUnit.SECONDS,
    durationUnit: TimeUnit = TimeUnit.MILLISECONDS)
  extends ScheduledReporter(
    registry: MetricRegistry,
    name: String,
    filter: MetricFilter,
    rateUnit: TimeUnit,
    durationUnit: TimeUnit) {

  override def report(
     gauges: SortedMap[String, Gauge[_]],
     counters: SortedMap[String, Counter],
     histograms: SortedMap[String, Histogram],
     meters: SortedMap[String, Meter],
     timers: SortedMap[String, Timer]): Unit = {
    gauges
      .filter(_._1 contains "monitoring.")
      .foreach{case (name, gauge) =>
      val Array(sourceId, monitoringId) = name.split("-monitoring.")
      gauge.getValue.asInstanceOf[Option[MonitoringData[_]]] match {
        case Some(data) => MonitoringTestingData.add(monitoringId, data)
        case None => // do nothing
      }
    }
  }

}

import collection.mutable.{HashMap, MultiMap, Set}

object MonitoringTestingData {

  private val allData = new HashMap[String, Set[MonitoringData[_]]] with MultiMap[String, MonitoringData[_]]

  def all[T](monitoringId: String): List[MonitoringData[T]] = allData(monitoringId).toList.sortBy(_.timestamp).map(_.asInstanceOf[MonitoringData[T]])
  def latest[T](monitoringId: String) : MonitoringData[T] = all(monitoringId).last

  def add(monitoringId: String, data: MonitoringData[_]): Unit = {
    allData.addBinding(monitoringId, data)
  }

  def clear(): Unit = allData.clear()

}