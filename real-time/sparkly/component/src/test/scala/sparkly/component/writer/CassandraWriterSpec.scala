package sparkly.component.writer

import scala.collection.JavaConversions._
import org.joda.time.DateTime
import sparkly.core._
import sparkly.testing._

class CassandraWriterSpec extends ComponentSpec with SparklyEmbeddedCassandra {

  "Cassandra writer" should "write features to cassandra" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Cassandra store",
      clazz = classOf[CassandraWriter].getName,
      inputs = Map (
        "In" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("stationid", "timestamp", "temperature")))
      ),
      properties = Map (
        "Keyspace" -> "cassandra_store_spec",
        "Table" -> "temperature",
        "Hosts" -> cassandraHost.getHostAddress,
        "Parallelism" -> "1"
      )
    )

    cassandraConnector.withSessionDo{ session =>
      session.execute("DROP KEYSPACE IF EXISTS cassandra_store_spec")
      session.execute("CREATE KEYSPACE cassandra_store_spec WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE cassandra_store_spec.temperature (stationid TEXT, timestamp TIMESTAMP, temperature INT, PRIMARY KEY (stationid, timestamp))")
    }

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 8),
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:28", "temperature" -> 9),
      Instance("stationid" -> "1", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 19)
    )

    // Then
    eventually {
      cassandraConnector.withSessionDo{ session =>
        val station0Temperatures = session.execute("SELECT * FROM cassandra_store_spec.temperature WHERE stationid = '0'").all.map{row => List(row.getString(0), row.getDate(1), row.getInt(2))}
        val station1Temperatures = session.execute("SELECT * FROM cassandra_store_spec.temperature WHERE stationid = '1'").all.map{row => List(row.getString(0), row.getDate(1), row.getInt(2))}
        val temperatures = (station0Temperatures ++ station1Temperatures).toList
        temperatures should contain only (
          List("0", DateTime.parse("2015-02-19T21:47:18").toDate, 8),
          List("0", DateTime.parse("2015-02-19T21:47:28").toDate, 9),
          List("1", DateTime.parse("2015-02-19T21:47:18").toDate, 19)
        )
      }
    }
  }

}