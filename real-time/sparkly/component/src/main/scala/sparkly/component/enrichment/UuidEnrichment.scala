package sparkly.component.enrichment

import org.apache.spark.streaming.dstream.DStream
import sparkly.core._
import sparkly.utils._

import scala.util.Random

class UuidEnrichment  extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "UUID",  category = "Data Enrichments",
    description = "Add uuid feature",
    inputs = Map("In" -> InputStreamMetadata(namedFeatures = Map("Date" -> FeatureType.DATE))),
    outputs = Map("Out" -> OutputStreamMetadata(from = Some("In"), namedFeatures = Map ("UUID" -> FeatureType.STRING)))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val out = context.dstream("In", "Out").map{ instance =>
      val uuid = Option(instance.inputFeature("Date").asDate) match {
        case Some(date) => UUIDGen.getTimeUUID(date.getTime, Random.nextLong).toString
        case None => UUIDGen.getTimeUUID.toString
      }

      instance.outputFeature("UUID", uuid)
    }

    Map("Out" -> out)
  }

}
