package sparkly.component.source

import java.sql._

import com.google.common.util.concurrent.RateLimiter

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import sparkly.component.utils._

import sparkly.core.PropertyType._
import sparkly.core.{ComponentMetadata, Context, OutputStreamMetadata, PropertyMetadata, _}
import resource._
import scala.collection.mutable.ListBuffer
import sparkly.core.FeatureType;
import sparkly.core.PropertyType._
import sparkly.utils.SparklyDirectoryStructure

class JdbcSource extends Component {

  override def metadata = ComponentMetadata(
    name = "JDBC source", description =
      """Fetch data from a relational database.
        |This source will query the db permanently using an increment field to ensure that only new data get fetched.
        |""".stripMargin,
    category = "Data Sources",
    outputs = Map(
      "Output" -> OutputStreamMetadata(listedFeatures = Map("Fields" -> FeatureType.STRING))
    ),
    properties = Map(
      "Url" -> PropertyMetadata(STRING),
      "Driver" -> PropertyMetadata(STRING),
      "User" -> PropertyMetadata(STRING),
      "Password" -> PropertyMetadata(STRING),
      "SQL query" -> PropertyMetadata(STRING, description = "SQL query used to fetch data. It should contains '$CONDITIONS' that will be replace by the incremented value (i.e. SELECT id, date, text, from, to FROM message WHERE $CONDITIONS LIMIT 100)"),
      "Increment field" -> PropertyMetadata(STRING, description = "Fields use as increment value (i.e date)"),
      "Start value" -> PropertyMetadata(LONG, defaultValue = Some(0L)),
      "Rate limit (query/second)" -> PropertyMetadata(INTEGER, defaultValue = Some(10)),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(1), description = "Level of parallelism to use")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val url = context.property("Url").as[String]
    val driver = context.property("Driver").as[String]
    val user = context.property("User").as[String]
    val password = context.property("Password").as[String]
    val sql = context.property("SQL query").as[String]
    val incrementField = context.property("Increment field").as[String]
    val startValue = context.property("Start value").as[Long]
    val rateLimit = context.property("Rate limit (query/second)").as[Int]
    val partitions = context.property("Parallelism").as[Int]
    val outputMapper = Some(context.outputMappers("Output"))
    val hadoopConf = SerializableHadoopConfiguration.from(context)

    val rawStreams =  (0 until partitions).map{ partition =>
      context.ssc.receiverStream(new SqlReceiver(context.pipelineDirectory, context.componentId, url, driver, user, password, sql, incrementField, startValue, rateLimit, partition, partitions, hadoopConf))
    }

    Map("Output" ->  context.ssc.union(rawStreams).map(rawData => new Instance(outputMapper = outputMapper).outputFeatures("Fields", rawData.values.toList))
    )
  }

}

class SqlReceiver(
  pipelineDirectory: String,
  componentId: String,
  url: String,
  driver: String,
  user: String,
  password: String,
  sql: String,
  incrementField: String,
  startValue: Long,
  rateLimit: Int,
  partition: Int, partitions: Int,
  hadoopConfiguration: SerializableHadoopConfiguration) extends Receiver[Map[String, String]](StorageLevel.MEMORY_AND_DISK_SER) with Logging {

  private val statePath = SparklyDirectoryStructure.getStateDirectory(pipelineDirectory, s"$componentId-sql-receiver-$partition")
  private val lowerBoundState = new HdfsState[Long](statePath, hadoopConfiguration)

  private var lowerBound: Long = _

  def onStart() {
    lowerBound = lowerBoundState.getOrElse(startValue)
    new Thread("SQL receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  private def receive() {
    try {
      managed(JdbcUtil.getConnection(driver, url, user, password)) acquireAndGet { connection =>
        val rateLimiter = RateLimiter.create(rateLimit)
        while (!isStopped) {
          rateLimiter.acquire()

          val data = fetchData(connection)
          store(data.iterator)

          updateLowerBound(connection, data)
        }
      }
    } catch {
      case t: Throwable => stop(s"Error while connecting to ${url}", t)
    }
  }

  private def fetchData(connection: Connection): List[Map[String, String]] = managed(connection.createStatement) acquireAndGet { statement =>
    val data = ListBuffer[Map[String, String]]()
    val conditions = s"$incrementField > $lowerBound AND MOD($incrementField, $partitions) = $partition"

    val query = sql.replace("$CONDITIONS", conditions)
    val result = statement.executeQuery(query)

    while(result.next) {
      val row = JdbcUtil.getData(result)
      data += row
    }

    data.toList
  }

  private def updateLowerBound(connection: Connection, data: List[Map[String, String]]) = if (!data.isEmpty) {
    lowerBound = data.map(_(incrementField).toLong).max
    lowerBoundState.set(lowerBound)
  }
}

object JdbcUtil {

  def getConnection(driver: String, url: String, user: String, password: String): Connection = {
    DriverManager.registerDriver(Class.forName(driver).newInstance.asInstanceOf[Driver])

    val connection = DriverManager.getConnection(url, user, password)
    connection.setAutoCommit(true)
    connection
  }

  def getData(rs: ResultSet): Map[String, String] = {
    val metadata = rs.getMetaData()
    (1 to metadata.getColumnCount).map(i => metadata.getColumnName(i) -> rs.getString(i)).toMap
  }

}