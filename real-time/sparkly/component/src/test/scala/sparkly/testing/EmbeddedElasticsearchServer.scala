package sparkly.testing

import java.nio.file.Files

import com.sksamuel.elastic4s.ElasticClient
import org.apache.commons.io.FileUtils
import org.apache.spark.Logging
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder._
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

class EmbeddedElasticsearchServer(val clusterName: String) extends Logging {
  val dataDir = Files.createTempDirectory("embedded_es_data_").toFile
  val settings = ImmutableSettings.settingsBuilder
    .put("node.data", "true")
    .put("path.data", dataDir.toString)
    .put("cluster.name", clusterName)
    .build

  private lazy val node = nodeBuilder().settings(settings).build
  def client: Client = node.client

  val host = "localhost"
  val port = 9300
  val hosts = host + ":" + port

  def start(): Unit = {
    logInfo("Starting embedded ES server")
    node.start()
  }

  def stop(): Unit = {
    logInfo("Stopping embedded ES server")
    node.close()
    try {
      FileUtils.forceDelete(dataDir)
    } catch {
      case e: Exception => logWarning("Unable to delete temporary data directory for ES.", e)
    }
  }

  def createIndex(index: String): Unit = {
    client.admin.indices.prepareCreate(index).execute.actionGet()
    client.admin.cluster.prepareHealth(index).setWaitForActiveShards(1).execute.actionGet()
  }
}

trait EmbeddedElasticsearch extends FlatSpec with BeforeAndAfterEach {

  val clusterName = "embedded-elasticsearch"

  val elasticsearchServer = new EmbeddedElasticsearchServer(clusterName)
  val elasticsearchClient: ElasticClient = new ElasticClient(elasticsearchServer.client)


  override def beforeEach() {
    super.beforeEach()
    elasticsearchServer.start()
  }

  override def afterEach() {
    super.afterEach()
    elasticsearchServer.stop()
  }
}