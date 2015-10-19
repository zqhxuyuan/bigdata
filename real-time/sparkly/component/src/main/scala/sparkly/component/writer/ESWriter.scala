package sparkly.component.writer

import org.elasticsearch.common.settings.ImmutableSettings

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._

import org.apache.spark.{Logging, TaskContext}
import org.apache.spark.streaming.dstream._

import sparkly.core._
import sparkly.core.PropertyType._

class ESWriter extends Component {

  override def metadata = ComponentMetadata (
    name = "Elasticsearch writer", description = "Write stream into Elasticsearch.",
    category = "Writers",
    inputs = Map (
      "In" -> InputStreamMetadata(listedFeatures = Map("Features" -> FeatureType.ANY))
    ),
    properties = Map (
      "Hosts" -> PropertyMetadata(PropertyType.STRING, description = "Comma separated list of remote with ports (i.e host:port,host:port,...)"),
      "Cluster name" -> PropertyMetadata(PropertyType.STRING, mandatory = false),
      "Index" -> PropertyMetadata(PropertyType.STRING),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val featureNames = context.inputFeatureNames("In", "Features")
    val hosts = context.property("Hosts").as[String]
    val clusterName = Some(context.property("Cluster name").as[String])
    val indexName = context.property("Index").as[String]
    val parallelism = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)

    val writer = new ESJobWriter(hosts, clusterName, indexName)

    context
      .dstream("In")
      .repartition(parallelism)
      .map(instance => (featureNames zip instance.inputFeatures("Features").asRaw).toMap)
      .foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))

    Map()
  }

}


class ESJobWriter(hosts: String, clusterName: Option[String], indexName: String) extends Serializable with Logging {

  def write(taskContext: TaskContext, data: Iterator[Map[String, Any]]) {
    val client = createClient()
    taskContext.addTaskCompletionListener((context: TaskContext) => client.close())

    val operations = data.map(features => index into indexName fields features).toSeq
    if(!operations.isEmpty) {
      client.execute{ bulk(operations) }.await
    }
  }

  def createClient(): ElasticClient = {
    val uri = ElasticsearchClientUri(s"elasticsearch://$hosts")

    val settings = ImmutableSettings.settingsBuilder()
    if(clusterName.isDefined) {
      settings.put("cluster.name", clusterName.get)
    }

    ElasticClient.remote(settings.build(), uri)
  }
}