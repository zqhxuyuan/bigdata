package sparkly.component.misc

import java.util

import io.gatling.jsonpath.JsonPath
import org.apache.spark.streaming.dstream.DStream
import org.codehaus.jackson.map.ObjectMapper
import sparkly.core.PropertyType._
import sparkly.core.{Context, InputStreamMetadata, OutputStreamMetadata, PropertyMetadata, PropertyType, _}

import scala.util.Try

class JsonParser extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata(
    name = "JSON parser", category = "Miscellaneous",
    description = "Parse a JSON string to extract",
    properties = Map(
      "Queries" -> PropertyMetadata(PropertyType.STRING, description = "Semicolon separated list of json-path expressions (see http://goessner.net/articles/JsonPath/)"),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    ),
    inputs = Map("Input" -> InputStreamMetadata(namedFeatures = Map("JSON" -> FeatureType.STRING))),
    outputs = Map("Output" -> OutputStreamMetadata(listedFeatures = Map("Values" -> FeatureType.STRING)))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val queries = context.property("Queries").as[String].split(";")
    val parallelism = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)

    val output = context.dstream("Input", "Output").repartition(parallelism).map{ instance =>
      val values = JsonParser.extractValues(instance.inputFeature("JSON").asString, queries)
      instance.outputFeatures("Values", values)
    }

    Map("Output" -> output)
  }
}

object JsonParser {
  def extractValues(json: String, queries: Iterable[String]): List[String] = {
      val mapper = new ObjectMapper
      val jsonObject = mapper.readValue(json, classOf[Object])

      val results = queries.map { q => Try {
        JsonPath.query(q, jsonObject).right.get.toList.head match {
          case col: util.Collection[_] => mapper.writeValueAsString(col)
          case map: util.Map[_, _] => mapper.writeValueAsString(map)
          case anythingElse => anythingElse.toString
        }
      }.getOrElse(null)
    }

    results.toList
  }
}