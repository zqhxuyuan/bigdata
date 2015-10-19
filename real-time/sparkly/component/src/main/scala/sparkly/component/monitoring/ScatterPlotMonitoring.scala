package sparkly.component.monitoring

import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.PropertyType._
import sparkly.core._
import sparkly.math._
import sparkly.math.stat.{ReservoirSampling, Bin, Histogram}

import scala.util.Try

class ScatterPlotMonitoring extends Component {

  override def metadata = ComponentMetadata (
    name = "Scatter plot", category = "Monitoring",
    description = "Scatter plot",
    properties = Map(
      "Window length (in ms)" -> PropertyMetadata(PropertyType.LONG, defaultValue = Some(SparkDefaultConfiguration.defaultBatchDurationMs)),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to avoid repartitioning."),
      "Bins" -> PropertyMetadata(INTEGER, defaultValue = Some(10)),
      "Compression" -> PropertyMetadata(INTEGER, defaultValue = Some(100))
    ),
    inputs = Map("In" -> InputStreamMetadata(listedFeatures = Map("Number features" -> FeatureType.NUMBER))),
    monitorings = Map("Scatter plot" -> MonitoringMetadata("scatter-plot"))
  )

  override def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val dstream = context.dstream("In")
    val featureNames = context.inputFeatureNames("In", "Number features")
    val windowDuration = context.properties("Window length (in ms)").as[Long]
    val partitions = context.property("Parallelism").as[Int]
    val bins = context.properties("Bins").as[Int]
    val compression = context.properties("Compression").as[Int]
    val monitoring = context.createMonitoring[ScatterPlotData]("Scatter plot")

    val stream = if(partitions < 1) dstream else dstream.repartition(partitions)

    stream
      .mapPartitions{ instances =>
        val features = instances.map(_.inputFeatures("Number features")).filter(!_.containsUndefined).map(_.asDoubleList).toList
        if(features.isEmpty) {
          Iterator()
        }  else {
          val plot = features.foldLeft(ScatterPlot(compression, featureNames))(_ add _)
          Iterator(plot)
        }
      }
      .reduceByWindow(_+_, Milliseconds(windowDuration), dstream.slideDuration)
      .foreachRDD{(rdd, time) =>
        Try(rdd.take(1)(0)).toOption match {
          case Some(model) => monitoring.add(model.data(bins))
          case None => // Send nothing
        }
      }

    Map()
  }

}

object ScatterPlot {
  def apply(compression: Int, features: List[String]): ScatterPlot = {
    val histograms = (0 until features.size).map(i => Histogram(compression)).toList
    val reservoir = ReservoirSampling[List[Double]](compression)
    ScatterPlot(compression, features, histograms, reservoir)
  }
}

case class ScatterPlot(compression: Int, features: List[String], histograms: List[Histogram], reservoir: ReservoirSampling[List[Double]]) {

  def add(values: List[Double]): ScatterPlot = {
    val newHistograms = histograms.zipWithIndex.map{case (hist, index) => hist.add(values(index))}
    this.copy(histograms = newHistograms, reservoir = this.reservoir.add(values))
  }

  def +(other: ScatterPlot): ScatterPlot = {
    val newHistograms = (this.histograms, other.histograms).zipped.map{case (hist1, hist2) => hist1 + hist2}
    this.copy(histograms = newHistograms, reservoir = this.reservoir + other.reservoir)
  }

  def data(bins: Int): ScatterPlotData = {
    val histograms = this.histograms.zipWithIndex.map{ case (hist, i) => HistogramData(features(i), hist.bins(bins))}
    ScatterPlotData(histograms, reservoir.samples)
  }
}

case class ScatterPlotData(histograms: List[HistogramData], samples: List[List[Double]])
case class HistogramData(feature: String, bins: List[Bin])