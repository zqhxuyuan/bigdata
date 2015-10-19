package sparkly.component.preprocess

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.PropertyType
import sparkly.core.PropertyType._

import sparkly.core._
import org.apache.spark.rdd.RDD


class Inputer extends Component {

  def metadata = ComponentMetadata(
    name = "Inputer", category = "Pre-processor",
    description = "Handle missing number values.",
    inputs = Map (
      "Input" -> InputStreamMetadata(listedFeatures = Map("Features" -> FeatureType.NUMBER))
    ),
    outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"))
    ),
    properties = Map(
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level."),
      "Missing" -> PropertyMetadata(PropertyType.STRING, mandatory = false, defaultValue = Some(""), description = "Placeholder for the missing values. Empty features and all occurrences of this value will be handled."),
      "Strategy" -> PropertyMetadata(PropertyType.STRING, defaultValue = Some("Mean"), acceptedValues = List("Mean", "Default", "Filter")),
      "Default value" -> PropertyMetadata(PropertyType.DECIMAL, mandatory = false, description = "Default value use to impute missing value when 'Default' strategy is choose")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val stream = context.dstream("Input", "Output")
    val partitions = context.property("Parallelism").as[Int] match {
      case positive if positive > 0 => positive
      case _ => context.sc.defaultParallelism
    }
    val detector = MissingValueDetector(context.property("Missing").as[String] match {
      case "" => None
      case "NaN" => Some(Double.NaN)
      case _ @str => Some(str.toDouble)
    })

    val out = context.property("Strategy").as[String] match {
      case "Mean" => initMeanStrategyStream(stream, detector, partitions)
      case "Default" => initDefaultStrategyStream(stream, detector, context.property("Default value").as[Double])
      case "Filter" => initFilterStrategyStream(stream, detector)
    }

    Map("Output" -> out)
  }

  private def initMeanStrategyStream(stream: DStream[Instance], detector: MissingValueDetector, partitions: Int) : DStream[Instance] = {
    var means = RunningMeans()

    stream
      .flatMap(i => i.inputFeatures("Features").values.zipWithIndex)
      .filter{case (f, index) => detector.isDefined(f)}
      .map{case (f, index) => (index, RunningMean(1L, f.asDouble))}
      .reduceByKey(_ + _, numPartitions = partitions)
      .foreachRDD{ rdd: RDD[(Int, RunningMean)] =>
        means = means.update(rdd.collect())
      }

    stream.map{ i =>
      val features = i.inputFeatures("Features").values.zipWithIndex.map{case (f, index) => if(detector.isMissing(f)) means(index) else f.asDouble}
      i.inputFeatures("Features", features)
    }
  }

  private def initDefaultStrategyStream(stream: DStream[Instance], detector: MissingValueDetector, default: Double) : DStream[Instance] = {
    stream.map{ instance =>
      val features = instance.inputFeatures("Features").values.map(f => if(detector.isMissing(f)) default else f.asDouble)
      instance.inputFeatures("Features", features)
    }
  }

  private def initFilterStrategyStream(stream: DStream[Instance], detector: MissingValueDetector) : DStream[Instance] = {
    stream.filter(i => !i.inputFeatures("Features").values.exists(f => detector.isMissing(f)))
  }

}

case class RunningMeans(means: Map[Int, RunningMean] = Map()) {
  def apply(index: Int): Double = means.getOrElse(index, RunningMean()).mean

  def update(index: Int, mean: RunningMean): RunningMeans = {
    val updatedMean = means.getOrElse(index, RunningMean()) + mean
    this.copy(means = this.means + (index -> updatedMean))
  }

  def update(values: Iterable[(Int, RunningMean)]): RunningMeans = {
    values.foldLeft(this)((current, t) => current.update(t._1, t._2))
  }
}

case class RunningMean(n: Long = 0L, sum: Double = 0.0) {
  def +(other: RunningMean): RunningMean = this.copy(n = this.n + other.n, sum = this.sum + other.sum)
  def +(values: Seq[Double]): RunningMean = this.copy(n = n + values.size, sum = sum + values.sum)
  def mean: Double = sum / n
}

case class MissingValueDetector(missing: Option[Double]) {
  def isMissing(feature: Feature[_]): Boolean = feature.isEmpty || (missing.isDefined && feature.asDouble == missing.get)
  def isDefined(feature: Feature[_]) = !isMissing(feature)
}