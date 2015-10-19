package sparkly.testing

import _root_.kafka.producer._
import _root_.kafka.server._
import kafka.utils.{ZKStringSerializer, TestUtils}
import kafka.admin.AdminUtils
import kafka.server.KafkaConfig
import org.I0Itec.zkclient.ZkClient
import org.apache.curator.test._
import scala.util.Random
import kafka.producer.KeyedMessage
import java.util.Properties
import org.scalatest._
import kafka.consumer._
import kafka.serializer._
import java.util.concurrent.Executors
import scala.collection.mutable.ListBuffer

trait EmbeddedKafka extends FlatSpec with BeforeAndAfterEach {

  var kafkaServer: KafkaServer = _

  override def beforeEach() {
    super.beforeEach()
    kafkaServer = new KafkaServer()
    kafkaServer.startZkKafkaCluster()
  }

  override def afterEach() {
    super.afterEach()
    kafkaServer.stopZkKafkaCluster()
  }

}

class KafkaServer(val kafkaPort: Int = 9092, val zkPort: Int = 2181) {

  val zkServer = new TestingZooKeeperServer(KafkaServer.getZkConfig(zkPort))
  val kafkaServer = new KafkaServerStartable(KafkaServer.getKafkaConfig(kafkaPort, zkServer.getInstanceSpec.getConnectString))
  val producer = new Producer[String, String](KafkaServer.getKafkaProducerConfig(kafkaBroker, zkServer.getInstanceSpec.getConnectString))
  val connectors = ListBuffer[ConsumerConnector]()

  def startZkKafkaCluster() {
    zkServer.start()
    kafkaServer.startup()
  }

  def stopZkKafkaCluster() {
    connectors.foreach(_.shutdown())
    producer.close()
    kafkaServer.shutdown()
    zkServer.close()
    kafkaServer.awaitShutdown()
  }

  def kafkaBroker = s"localhost:${kafkaPort}"
  def zkConnectString = zkServer.getInstanceSpec.getConnectString

  def send(topic: String, key: String, message: String) {
    val keyedMessage = new KeyedMessage[String, String](topic, key, message)
    producer.send(keyedMessage)
  }

  def send(topic: String, message: String, others: String*) {
    (message :: others.toList).foreach(str => producer.send(new KeyedMessage[String, String](topic, str)))
  }

  def createTopic(topic: String, numPartitions: Int = 1, replicationFactor: Int = 1): Unit = {
    val zkClient = new ZkClient(zkServer.getInstanceSpec.getConnectString, 10000, 10000, ZKStringSerializer)
    AdminUtils.createTopic(zkClient, topic, numPartitions, replicationFactor)
    zkClient.close()
  }

  def listen(topic: String, groupId: String): ListBuffer[Array[Byte]]= {
    val data = ListBuffer[Array[Byte]]()
    val connector = Consumer.create(KafkaServer.createConsumerConfig(zkConnectString, groupId))
    connectors += connector

    val streams = connector
      .createMessageStreams(Map(topic -> 1), new StringDecoder(), new DefaultDecoder())
      .get(topic)

    val executor = Executors.newFixedThreadPool(2)

    // consume the messages in the threads
    for (stream <- streams) {
      executor.submit(new Runnable() {
        def run() {
          for (s <- stream) {
            while (s.iterator.hasNext) {
              data += s.iterator.next.message
            }
          }
        }
      })
    }

    data
  }

}

object KafkaServer {

  def getKafkaConfig(kafkaPort: Int, zkConnectString: String): KafkaConfig = {
    val props = TestUtils.createBrokerConfigs(1).iterator.next
    props.put("zookeeper.connect", zkConnectString)
    props.put("port", kafkaPort.toString)
    props.put("auto.create.topics.enable", "true")
    new KafkaConfig(props);
  }

  def getKafkaProducerConfig(kafkaBroker: String, zkConnect: String): ProducerConfig = {
    val props = new Properties();
    props.put("metadata.broker.list", kafkaBroker)
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("message.send.max.retries", "5")
    props.put("retry.backoff.ms", "500")
    new ProducerConfig(props);
  }

  def createConsumerConfig(zkConnect: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    return new ConsumerConfig(props)
  }

  def getZkConfig(zkPort: Int): QuorumConfigBuilder = {
    val spec = new InstanceSpec(null, zkPort, -1, -1, true, -1)
    new QuorumConfigBuilder(spec)
  }

  def randomPort(min: Int, max: Int): Int = {
    val range = min to max
    range(Random.nextInt(range.length))
  }
}
