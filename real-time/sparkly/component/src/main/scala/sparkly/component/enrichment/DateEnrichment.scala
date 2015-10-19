package sparkly.component.enrichment

import sparkly.core._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.Context
import java.util.{TimeZone, Date}
import com.github.nscala_time.time.Imports._
import org.joda.time.format.ISODateTimeFormat

class DateEnrichment extends Component {

  TimeZone.setDefault(DateTimeZone.UTC.toTimeZone)

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Date time",  category = "Data Enrichments",
    description = "Add time features",
    inputs = Map("In" -> InputStreamMetadata(namedFeatures = Map("Date" -> FeatureType.DATE))),
    outputs = Map("Out" -> OutputStreamMetadata(from = Some("In"), namedFeatures = Map (
      "Timestamp ms" -> FeatureType.LONG,
      "Timestamp unix" -> FeatureType.LONG,
      "ISO-8601 format" -> FeatureType.STRING,
      "Year" -> FeatureType.STRING,
      "Month" -> FeatureType.STRING,
      "Day" -> FeatureType.STRING,
      "Hour" -> FeatureType.STRING,
      "Minute" -> FeatureType.STRING,
      "Second" -> FeatureType.STRING,
      "Day of week" -> FeatureType.INTEGER
    )))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val dateFeatureExists = context.inputFeatureMapped("In", "Date")

    val inputs = if(dateFeatureExists) {
      context.dstream("In", "Out").map(instance => (instance, instance.inputFeature("Date").asDate))
    } else {
      context.dstream("In", "Out").transform((rdd, time) => rdd.map(instance => (instance, new Date(time.milliseconds))))
    }

    val out = inputs.map{case (instance, date) =>
      if(date != null) {
        val dateTime = new DateTime(date)
        instance.outputFeatures(
          "Timestamp ms" -> dateTime.getMillis,
          "Timestamp unix" -> dateTime.getMillis / 1000L,
          "ISO-8601 format" -> dateTime.toString(ISODateTimeFormat.dateTime.withZoneUTC()),
          "Year" -> dateTime.toString(DateTimeFormat.forPattern("yyyy").withZoneUTC()),
          "Month" -> dateTime.toString(DateTimeFormat.forPattern("MM").withZoneUTC()),
          "Day" -> dateTime.toString(DateTimeFormat.forPattern("dd").withZoneUTC()),
          "Hour" -> dateTime.toString(DateTimeFormat.forPattern("HH").withZoneUTC()),
          "Minute" -> dateTime.toString(DateTimeFormat.forPattern("mm").withZoneUTC()),
          "Second" -> dateTime.toString(DateTimeFormat.forPattern("ss").withZoneUTC()),
          "Day of week" -> dateTime.getDayOfWeek
        )
      } else {
        instance.outputFeatures(
          "Timestamp ms" -> null,
          "Timestamp unix" -> null,
          "ISO-8601 format" -> null,
          "Year" -> null,
          "Month" -> null,
          "Day" -> null,
          "Hour" -> null,
          "Minute" -> null,
          "Second" -> null,
          "Day of week" -> null
        )
      }
    }

    Map("Out" -> out)
  }
}
