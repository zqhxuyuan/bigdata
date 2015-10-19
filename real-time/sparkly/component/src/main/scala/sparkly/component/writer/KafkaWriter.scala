package sparkly.component.writer

import java.util.Properties

import kafka.producer._
import org.apache.spark.{Logging, TaskContext}
import org.apache.spark.streaming.dstream.DStream

import sparkly.core._
import sparkly.common._
import sparkly.core.PropertyType._

class KafkaWriter extends Component {

  override def metadata = ComponentMetadata(
    name = "Kafka writer", description = "Write stream to Kafka.",
    category = "Writers",
    inputs = Map(
      "In" -> InputStreamMetadata(listedFeatures = Map("Features" -> FeatureType.ANY))
    ),
    properties = Map(
      "Topic" -> PropertyMetadata(STRING),
      "Metadata broker list" -> PropertyMetadata(STRING, defaultValue = Some("hostname:port,hostname:port,..."), description = "Where the Producer can find a one or more Brokers to determine the Leader for each topic. Format hostname:port,hostname:port,..."),
      "Serializer" -> PropertyMetadata(PropertyType.STRING, acceptedValues = List("Json", "Kryo")),
      "Max retry" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(5)),
      "Retry backoff (ms)" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(50)),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val featureNames = context.inputFeatureNames("In", "Features")
    val parallelism = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)

    val topic = context.property("Topic").as[String]
    val brokerList = context.property("Metadata broker list").as[String]
    val maxRetry = context.property("Max retry").as[Int]
    val retryBackoffMs = context.property("Retry backoff (ms)").as[Int]
    val serializer = context.property("Serializer").as[String] match {
      case "Json" => new JsonSerializer[Map[String, Any]]()
      case "Kryo" => new KryoSerializer[Map[String, Any]]()
    }

    val writer = new KafkaJobWriter(serializer, topic, brokerList, maxRetry, retryBackoffMs)

    context
      .dstream("In")
      .repartition(parallelism)
      .map(instance => (featureNames zip instance.inputFeatures("Features").asRaw).toMap)
      .foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))

    Map()
  }
}

class KafkaJobWriter(serializer: Serializer[Map[String, Any]], topic: String, brokerList: String, maxRetry: Int, retryBackoffMs: Long) extends Serializable with Logging {

  def write(taskContext: TaskContext, data: Iterator[Map[String, Any]]) {
    val producer = createProducer()
    taskContext.addTaskCompletionListener((context: TaskContext) => producer.close())

    data.foreach{ features =>
      val bytes = serializer.serialize(features)
      val keyedMessage = new KeyedMessage[String, Array[Byte]](topic, bytes)
      producer.send(keyedMessage)
    }
  }

  def createProducer(): Producer[String, Array[Byte]] = {
    val configuration = getKafkaProducerConfig
    new Producer[String, Array[Byte]](configuration)
  }

  def getKafkaProducerConfig(): ProducerConfig = {
    val props = new Properties();
    props.put("metadata.broker.list", brokerList)
    props.put("message.send.max.retries", maxRetry.toString)
    props.put("retry.backoff.ms", retryBackoffMs.toString)
    new ProducerConfig(props);
  }
}
