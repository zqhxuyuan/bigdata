package sparkly.component.monitoring

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.PropertyType._
import sparkly.core._
import sparkly.math.stat.FeatureSummary

import scala.util.Try

class FeatureStatisticsMonitoring extends Component {

  override def metadata = ComponentMetadata (
    name = "Feature statistics", category = "Monitoring",
    description = "Create feature statistics monitoring",
    properties = Map(
      "Window length (in ms)" -> PropertyMetadata(PropertyType.LONG, defaultValue = Some(SparkDefaultConfiguration.defaultBatchDurationMs)),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    ),
    inputs = Map("In" -> InputStreamMetadata(namedFeatures = Map("Number feature" -> FeatureType.NUMBER))),
    monitorings = Map("Feature statistics" -> MonitoringMetadata(ChartType.AREAS,
      values = List("max", "quantile 0.99", "quantile 0.90", "quantile 0.75", "quantile 0.50", "quantile 0.25", "min"),
      primaryValues = List("mean", "std", "missing", "count")
    ))
  )

  override def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val dstream = context.dstream("In")
    val windowDuration = context.properties("Window length (in ms)").as[Long]
    val partitions = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)
    val monitoring = context.createMonitoring[Map[String, Double]]("Feature statistics")

    dstream
      .repartition(partitions)
      .mapPartitions(instances => Iterator(FeatureSummary(instances.map(_.inputFeature("Number feature").asDoubleOr(Double.NaN)))))
      .reduceByWindow((stat1, stat2) => stat1.merge(stat2), Milliseconds(windowDuration), dstream.slideDuration)
      .foreachRDD((rdd, time) => {
        val stats = Try(rdd.take(1)(0)).getOrElse(FeatureSummary.zero())

        val data = Map(
          "count" -> stats.count.toDouble,
          "missing" -> stats.missing.toDouble,
          "mean" -> stats.mean,
          "std" -> stats.stdev,
          "min" -> stats.min,
          "max" -> stats.max,
          "quantile 0.25" -> stats.quantile(0.25),
          "quantile 0.50" -> stats.quantile(0.50),
          "quantile 0.75" -> stats.quantile(0.75),
          "quantile 0.90" -> stats.quantile(0.90),
          "quantile 0.99" -> stats.quantile(0.99)
        )
        monitoring.add(time.milliseconds, data)
      })

    Map()
  }
}

