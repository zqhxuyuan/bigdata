package sparkly.component.enrichment

import sparkly.testing._
import sparkly.core._
import com.github.nscala_time.time.Imports._
import org.apache.cassandra.utils.UUIDGen
import java.util.UUID

class UuidEnrichmentSpec extends ComponentSpec {

  "Uuid Enrichments" should "enrich uuid from stream with date" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Add UUID",
      clazz = classOf[UuidEnrichment].getName,
      inputs = Map("In" -> StreamConfiguration(mappedFeatures = Map("Date" -> "date"))),
      outputs = Map("Out" -> StreamConfiguration(mappedFeatures = Map("UUID" -> "uuid")))
    )

    // When
    val component = deployComponent(configuration)
    val instances = (1 to 1000).map(index => Instance("date" -> "2015-02-04T00:00:00.000Z"))
    component.inputs("In").push(instances.toList)

    // Then
    eventually {
      val uuids = component.outputs("Out").features.map(data => data("uuid").asInstanceOf[String])
      uuids.toSet should have size (1000)

      uuids.foreach{ uuid =>
        UUIDGen.getAdjustedTimestamp(UUID.fromString(uuid)) should equal (DateTime.parse("2015-02-04T00:00:00.000Z").getMillis)
      }
    }
  }


  "Uuid Enrichments" should "enrich stream without date" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Add UUID",
      clazz = classOf[UuidEnrichment].getName,
      inputs = Map("In" -> StreamConfiguration(mappedFeatures = Map())),
      outputs = Map("Out" -> StreamConfiguration(mappedFeatures = Map("UUID" -> "uuid")))
    )

    // When
    val component = deployComponent(configuration)
    val instances = (1 to 1000).map(index => Instance())
    component.inputs("In").push(instances.toList)

    // Then
    eventually {
      val uuids = component.outputs("Out").features.map(data => data("uuid").asInstanceOf[String])
      uuids.toSet should have size (1000)

      uuids.foreach{ uuid =>
        UUIDGen.getAdjustedTimestamp(UUID.fromString(uuid)) should equal (System.currentTimeMillis +- 60 * 1000)
      }
    }
  }
}
