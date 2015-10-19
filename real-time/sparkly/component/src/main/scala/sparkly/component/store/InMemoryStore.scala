package sparkly.component.store

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core._

class InMemoryStore extends Component {

  override def metadata = ComponentMetadata (
    name = "In memory store", description = "Store features in a reliable in memory store.",
    category = "Stores",
    inputs = Map (
      "Update" -> InputStreamMetadata(listedFeatures = Map("Features" -> FeatureType.ANY), namedFeatures = Map("Id" -> FeatureType.STRING)),
      "Query" -> InputStreamMetadata(namedFeatures = Map("Id" -> FeatureType.ANY))
    ),
    outputs = Map (
      "Query result" -> OutputStreamMetadata(from = Some("Query"), listedFeatures = Map("Features" -> FeatureType.ANY))
    ),
    properties = Map (
      "Parallelism" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val parallelism = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)

    val updatedModels = context
      .dstream("Update")
      .map(instance => (instance.inputFeature("Id").asString, instance.inputFeatures("Features")))
      .updateStateByKey(InMemoryStore.updateState _, parallelism)

    val queryResults = context
      .dstream("Query", "Query result")
      .map(instance => (instance.inputFeature("Id").asString, instance))
      .join(updatedModels)
      .map{case(id, (instance, features)) => instance.outputFeatures("Features", features)}

    Map("Query result" -> queryResults)
  }
}

object InMemoryStore {
  def updateState(values: Seq[FeatureList], state: Option[List[_]]): Option[List[_]] = if(!values.isEmpty) {
    Some(values.last.asRaw)
  } else {
    state
  }
}