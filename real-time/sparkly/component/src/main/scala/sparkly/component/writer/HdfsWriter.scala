package sparkly.component.writer

import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.dstream.DStream
import sparkly.core._
import sparkly.common.JsonSerializer

class HdfsWriter extends Component {

  override def metadata = ComponentMetadata (
    name = "HDFS writer", description = "Write stream into hdfs (text or sequence files). The file name at each interval is generated based on 'prefix' and 'suffix' (\"prefix-TIME_IN_MS.suffix\").",
    category = "Writers",
    inputs = Map (
      "In" -> InputStreamMetadata(listedFeatures = Map("Features" -> FeatureType.ANY))
    ),
    properties = Map (
      "Location prefix" -> PropertyMetadata(PropertyType.STRING),
      "Location suffix" -> PropertyMetadata(PropertyType.STRING, mandatory = false, defaultValue = Some("csv")),
      "Interval (ms)" -> PropertyMetadata(PropertyType.LONG, defaultValue = Some(SparkDefaultConfiguration.defaultBatchDurationMs)),
      "Format" -> PropertyMetadata(PropertyType.STRING, defaultValue = Some("Text file (CSV)"), acceptedValues = List("Text file (JSON)", "Text file (CSV)", "Sequence file (Java serialization)")),
      "Parallelism" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val featureNames = context.inputFeatureNames("In", "Features")
    val prefix = context.property("Location prefix").as[String]
    val suffix = context.property("Location suffix").as[String]
    val window = context.property("Interval (ms)").or(context.dstream("In").slideDuration.milliseconds)
    val format = context.property("Format").as[String]
    val parallelism = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)

    val windowedStream = context.dstream("In")
      .repartition(parallelism)
      .window(Milliseconds(window), Milliseconds(window))

    format match {
      case "Text file (JSON)" => windowedStream.map(instance => HdfsWriter.toJson(featureNames, instance)).saveAsTextFiles(prefix, suffix)
      case "Text file (CSV)" => windowedStream.map(instance => HdfsWriter.toCsv(instance)).saveAsTextFiles(prefix, suffix)
      case "Sequence file (Java serialization)" => windowedStream.map(instance => HdfsWriter.toMap(featureNames, instance)).saveAsObjectFiles(prefix, suffix)
    }

    Map()
  }

}

object HdfsWriter {

  private val serializer = new JsonSerializer[Map[String, Any]]()

  def toCsv(instance: Instance): String = {
    val features = instance.inputFeatures("Features").asRaw
    features.map(_.toString).mkString(",")
  }

  def toJson(featureNames: List[String], instance: Instance): String = {
    val data = toMap(featureNames, instance)
    new String(serializer.serialize(data))
  }

  def toMap(featureNames: List[String], instance: Instance): Map[String, Any] = {
    val features = instance.inputFeatures("Features").asRaw
    (featureNames zip features).toMap
  }
}