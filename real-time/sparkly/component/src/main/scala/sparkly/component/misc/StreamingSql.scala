package sparkly.component.misc

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import sparkly.core._

class StreamingSql extends Component {
  
  val STREAM_COUNT = 4

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Streaming Sql", description = "Run SQL queries over a single stream",
    category = "Miscellaneous",
    inputs = (1 to STREAM_COUNT).map(i => "Stream" + i -> InputStreamMetadata(listedFeatures = Map("Fields" -> FeatureType.STRING))).toMap,
    outputs = Map("Output" -> OutputStreamMetadata(listedFeatures = Map("Fields" -> FeatureType.STRING))),
    properties = Map("Query" -> PropertyMetadata(PropertyType.STRING, description = "SQL query to run. (Stream tables are named 'stream1', 'stream2', ..."))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val query = context.property("Query").as[String]
    val outputFields = context.outputFeatureNames("Output", "Fields")

    val schemas = (1 to STREAM_COUNT).map(i => StructType(context.inputFeatureNames("Stream" + i, "Fields").map(fieldName => StructField(fieldName, StringType, true))))
    val inputs = (1 to STREAM_COUNT).map(i => context.dstream("Stream" + i).map(("stream" + i, _))).reduce(_ union _)

    val output = inputs
      .transform { rdd =>
        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

        (1 to STREAM_COUNT).foreach{ i =>
          // TODO filter(_._1 == "stream" + i) is not efficient
          val schema = schemas(i - 1)
          val rowRDD = rdd.filter(_._1 == "stream" + i).map(instance => Row.fromSeq(instance._2.inputFeatures("Fields").asStringList))
          sqlContext.createDataFrame(rowRDD, schema).registerTempTable("stream" + i)
        }

        val results = sqlContext.sql(query).map(row => Instance((outputFields zip row.toSeq).toMap))
        results.take(1) // We need such kind of operations however there's not output to transform!
        results
      }

    Map("Output" -> output)
  }
}

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}