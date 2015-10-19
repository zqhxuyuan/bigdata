package sparkly.component.monitoring

import sparkly.core._
import sparkly.core.PropertyType._
import sparkly.math.stat.PearsonCorrelations
import scala.Some
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Milliseconds
import scala.util.Try
import breeze.linalg.DenseMatrix

class CorrelationsMonitoring extends Component {

  override def metadata = ComponentMetadata(
    name = "Correlations", category = "Monitoring",
    description = "Create correlations monitoring",
    properties = Map(
      "Window length (in ms)" -> PropertyMetadata(PropertyType.LONG, defaultValue = Some(SparkDefaultConfiguration.defaultBatchDurationMs)),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    ),
    inputs = Map("In" -> InputStreamMetadata(listedFeatures = Map("Number features" -> FeatureType.NUMBER))),
    monitorings = Map("Correlations" -> MonitoringMetadata("Correlations"))
  )

  override def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val dstream = context.dstream("In")
    val windowDuration = context.properties("Window length (in ms)").as[Long]
    val partitions = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)
    val monitoring = context.createMonitoring[Map[String, Double]]("Correlations")
    val featureNames = context.inputFeatureNames("In", "Number features")

    dstream
      .repartition(partitions)
      .mapPartitions{ instances =>
        val features = instances.map(_.inputFeatures("Number features")).filter(!_.containsUndefined).map(_.toDenseVector(0.0))
        if(features.isEmpty) Iterator() else Iterator(PearsonCorrelations(features))
      }
      .reduceByWindow(_+_, _-_, Milliseconds(windowDuration), dstream.slideDuration)
      .foreachRDD{(rdd, time) =>
        Try(rdd.take(1)(0)).toOption match {
          case Some(correlation) => CorrelationsMonitoring.update(monitoring, featureNames, time.milliseconds, correlation.correlations())
          case None => // Send nothing
        }
      }

    Map()
  }

}

object CorrelationsMonitoring {
  def update(monitoring: Monitoring[Map[String, Double]], featureNames: List[String], timestamp: Long, correlations: DenseMatrix[Double]) {
    val data = correlations
      .iterator
      .map{case ((i, j), correlation) =>  (s"${featureNames(i)}.${featureNames(j)}", correlation)}
      .toMap

    monitoring.add(timestamp, data)
  }
}