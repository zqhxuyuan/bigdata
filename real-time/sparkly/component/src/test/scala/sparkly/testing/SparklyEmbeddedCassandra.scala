package sparkly.testing

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{UserDefinedProperty, EmbeddedCassandra}

trait SparklyEmbeddedCassandra extends EmbeddedCassandra {

  useCassandraConfig(Seq("cassandra-default.yaml.template"), forceReload = true)

  val cassandraHost = EmbeddedCassandra.getHost(0)
  val cassandraConnector = CassandraConnector(Set(cassandraHost))

  def clearCache(): Unit = CassandraConnector.evictCache()

}