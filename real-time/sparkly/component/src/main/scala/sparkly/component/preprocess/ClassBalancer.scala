package sparkly.component.preprocess

import org.apache.spark.streaming.dstream.DStream
import sparkly.math.stat.ReservoirSampling
import scala.Some
import sparkly.core._

class ClassBalancer extends Component {

  def metadata = ComponentMetadata (
    name = "Class balancer", category = "Pre-processor",
    description =
      """
        |Rebalance stream to reduce over-represented classes.
        |This component help dealing with imbalanced classes by sampling.
      """.stripMargin,
    inputs = Map (
      "Input" -> InputStreamMetadata(namedFeatures = Map("Class" -> FeatureType.ANY))
    ),
    outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"))
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val stream = context.dstream("Input", "Output")

    val sampled = stream.transform { rdd =>
      val grouped = rdd.groupBy(i => i.inputFeature("Class").asString)
      val proportions = grouped.map { case (clazz, instances) => instances.size}
      val minimum = proportions.min
      grouped.flatMap{ case (clazz, instances) => ReservoirSampling(minimum, instances).samples}
    }

    Map("Output" -> sampled)
  }

}
