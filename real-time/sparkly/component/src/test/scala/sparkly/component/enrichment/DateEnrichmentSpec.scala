package sparkly.component.enrichment

import sparkly.testing._
import sparkly.core._
import com.github.nscala_time.time.Imports._

class DateEnrichmentSpec extends ComponentSpec {

  "Date Enrichments" should "enrich date from stream" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Under test",
      clazz = classOf[DateEnrichment].getName,
      inputs = Map("In" -> StreamConfiguration(mappedFeatures = Map("Date" -> "date"))),
      outputs = Map("Out" -> StreamConfiguration(mappedFeatures = Map(
        "Timestamp ms" -> "timestamp ms",
        "Timestamp unix" -> "timestamp unix",
        "ISO-8601 format" -> "iso",
        "Year" -> "year",
        "Month" -> "month",
        "Day" -> "day",
        "Hour" -> "hour",
        "Minute" -> "minute",
        "Second" -> "second",
        "Day of week" -> "day of week"
      )))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(
      Instance("date" -> "2015-02-04T00:00:00.000Z"),
      Instance("date" -> DateTime.parse("2015-02-05T00:00:00.000Z").toDate),
      Instance("date" -> null)
    )

    // Then
    eventually {
      component.outputs("Out").features should contain only (
        Map("date" -> "2015-02-04T00:00:00.000Z", "timestamp ms" -> 1423008000000L, "timestamp unix" -> 1423008000L, "iso" ->  "2015-02-04T00:00:00.000Z", "year" -> "2015", "month" -> "02", "day" -> "04", "hour" -> "00", "minute" -> "00", "second" -> "00", "day of week" -> 3),
        Map("date" -> DateTime.parse("2015-02-05T00:00:00.000Z").toDate, "timestamp ms" -> 1423094400000L, "timestamp unix" -> 1423094400L, "iso" ->  "2015-02-05T00:00:00.000Z", "year" -> "2015", "month" -> "02", "day" -> "05", "hour" -> "00", "minute" -> "00", "second" -> "00", "day of week" -> 4),
        Map("date" -> null, "timestamp ms" -> null, "timestamp unix" -> null, "iso" ->  null, "year" -> null, "month" -> null, "day" -> null, "hour" -> null, "minute" -> null, "second" -> null, "day of week" -> null)
      )
    }
  }


  "Date Enrichments" should "enrich stream with rdd time" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Under test",
      clazz = classOf[DateEnrichment].getName,
      inputs = Map("In" -> StreamConfiguration(mappedFeatures = Map())),
      outputs = Map("Out" -> StreamConfiguration(mappedFeatures = Map(
        "Timestamp ms" -> "timestamp ms",
        "Timestamp unix" -> "timestamp unix",
        "ISO-8601 format" -> "iso",
        "Year" -> "year",
        "Month" -> "month",
        "Day" -> "day",
        "Hour" -> "hour",
        "Minute" -> "minute",
        "Second" -> "second",
        "Day of week" -> "day of week"
      )))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(Instance())

    // Then
    eventually {
      val rawFeatures = component.outputs("Out").features.toList(0)
      rawFeatures("timestamp ms").asInstanceOf[Long] should equal (System.currentTimeMillis +- 60 * 1000)
    }
  }
}
