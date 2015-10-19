package org.apache.spark.metrics

import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.Source
import com.codahale.metrics._

object SparklyMonitoringSource extends Source {
  private var registry: Option[MetricRegistry] = None

  def init(): MetricRegistry = {
    registry.foreach(r => r.removeMatching(MetricFilter.ALL))
    registry = Some(new MetricRegistry)
    registry.get
  }

  def sourceName: String =  s"sparkly-monitoring"
  def metricRegistry: MetricRegistry = registry match {
    case Some(registry) => registry
    case None => init()
  }

  def registerToMetricsSystem() = SparkEnv.get.metricsSystem.registerSource(this)
}
