package sparkly.component.preprocess

import org.apache.spark.streaming.dstream.DStream
import sparkly.core.PropertyType._
import sparkly.core._

class LabelEncoder extends Component {

  def metadata = ComponentMetadata (
    name = "Label encoder", category = "Pre-processor",
    description =
      """
        |Encode categorical labels to discrete values [0, nb classes - 1].
      """.stripMargin,
    inputs = Map (
      "Input" -> InputStreamMetadata(namedFeatures = Map("Label" -> FeatureType.CATEGORICAL))
    ),
    outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"), namedFeatures = Map("Discrete label" -> FeatureType.INTEGER))
    ),
    properties = Map (
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to avoid repartitioning.")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val partitions = context.property("Parallelism").as[Int]
    val stream = if(partitions < 1) context.dstream("Input", "Output") else context.dstream("Input", "Output").repartition(partitions)

    var categoryIndex = CategoryIndex()
    stream.map(i => i.inputFeature("Label").asString).foreachRDD { rdd =>
      categoryIndex = categoryIndex.add(rdd.distinct().collect())
    }

    val out = stream.map{ instance =>
      val index = categoryIndex(instance.inputFeature("Label").asString)
      instance.outputFeature("Discrete label", index)
    }

    Map("Output" -> out)
  }

}