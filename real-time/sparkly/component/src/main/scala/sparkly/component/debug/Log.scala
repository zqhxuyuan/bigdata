package sparkly.component.debug

import org.apache.spark.streaming.dstream.DStream
import sparkly.core.FeatureType.ANY
import sparkly.core._
import sparkly.core.PropertyType._

class Log extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Log",
    inputs = Map[String, InputStreamMetadata] (
      "Input" -> InputStreamMetadata(listedFeatures = Map("Features" -> ANY))
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val features = context.inputMappers("Input").featuresNames("Features")
    context
      .dstream("Input")
      .map(instance => instance.rawFeatures.filterKeys(features.contains).toString)
      .print()

    Map()
  }
}