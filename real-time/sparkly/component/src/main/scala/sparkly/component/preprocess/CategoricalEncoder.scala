package sparkly.component.preprocess

import org.apache.spark.streaming.dstream.DStream
import sparkly.core.FeatureType.{CATEGORICAL, VECTOR}
import sparkly.core.PropertyType.INTEGER
import sparkly.core._
import org.apache.spark.Logging
import breeze.linalg.DenseVector

class CategoricalEncoder extends Component with Logging {

  def metadata = ComponentMetadata (
    name = "Categorical encoder", category = "Pre-processor",
    description =
      """
        |Encode categorical features using One-hot/One-of-k encoding scheme.
        |It will transform a categorical feature into 'k' binary features (0.0 and 1.0), with only one active.
        |'k' is the number of distinct categories.
      """.stripMargin,
    inputs = Map (
      "Input" -> InputStreamMetadata(namedFeatures = Map("Feature" -> CATEGORICAL))
    ),
    outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"), namedFeatures = Map("Encoded features" -> VECTOR))
    ),
    properties = Map(
      "K" -> PropertyMetadata(INTEGER, description = "Number of distinct categories"),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val k = context.property("K").as[Int]
    val partitions = context.property("Parallelism").as[Int] match {
      case positive if positive > 0 => positive
      case _ => context.sc.defaultParallelism
    }

    val stream = context.dstream("Input", "Output")

    var categoryIndex = CategoryIndex()
    stream.map(i => i.inputFeature("Feature").asString).foreachRDD{ rdd =>
      categoryIndex = categoryIndex.add(rdd.distinct(partitions).collect())
    }

    val out = stream.map{ instance =>
      val encoded = Array.fill(k)(0.0)

      val index = categoryIndex(instance.inputFeature("Feature").asString).toInt
      if(index < k) {
        encoded(index.toInt) = 1.0
      } else {
        logError(s"Got $index categories but encoder is limited to $k categories")
      }

      instance.outputFeature("Encoded features", DenseVector(encoded))
    }

    Map("Output" -> out)
  }

}