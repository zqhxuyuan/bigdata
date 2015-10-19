package sparkly.component.misc

import scala.Some
import sparkly.core._
import org.apache.spark.streaming.dstream.DStream

class VectorExtractor extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Vector extractor", category = "Miscellaneous",
    description = "Get values from vectors",
    inputs = Map ("Input" -> InputStreamMetadata(namedFeatures = Map("Vector" -> FeatureType.VECTOR))),
    outputs = Map("Output" -> OutputStreamMetadata(from = Some("Input"), listedFeatures = Map("Features" -> FeatureType.DOUBLE)))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val size = context.outputSize("Output", "Features")

    val out = context.dstream("Input", "Output").map { instance =>
      val vector = instance.inputFeature("Vector").asVector
      val data = vector.toArray.slice(0, size)
      instance.outputFeatures("Features", data)
    }

    Map("Output" -> out)
  }

}
