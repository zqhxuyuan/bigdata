package sparkly.component.source

import sparkly.core._
import sparkly.core.PropertyType._

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.serializer.StringDecoder

class KafkaSource extends Component {

  override def metadata = ComponentMetadata (
    name = "Kafka stream", description = "Reads messages from a topic in Kafka.",
    category = "Data Sources",
    outputs = Map(
      "Output" -> OutputStreamMetadata(namedFeatures = Map("Message" -> FeatureType.STRING))
    ),
    properties = Map(
      "Topics" -> PropertyMetadata(STRING, description = "Comma separated list of topics"),
      "Group Id" -> PropertyMetadata(STRING),
      "Metadata broker list" -> PropertyMetadata(STRING, defaultValue = Some("localhost:9092"), description = "Format hostname:port,hostname:port,...")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val topics = context.property("Topics").as[String].split(",").toSet
    val group = context.property("Group Id").as[String]
    val brokers = context.property("Metadata broker list").as[String]
    val featureName = context.outputFeatureName("Output", "Message")

    val kafkaParams = Map (
      "metadata.broker.list" -> brokers,
      "group.id" -> group
    )

    val out = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](context.ssc, kafkaParams, topics)
      .map{case (key, message) => Instance(featureName -> message)}

    Map("Output" -> out)
  }
}
