package sparkly.component.writer

import org.apache.commons.lang.SerializationUtils
import sparkly.core._
import sparkly.testing._

import scala.io.Source
import scala.reflect.io.Directory
import sparkly.common.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper

class HdfsWriterSpec extends ComponentSpec {

  "Hdfs writer" should "write features to hdfs using Java serialization" in {

    val workingDirectory = Directory.makeTemp("hdfs-write-spec")

    val configuration = ComponentConfiguration (
      name = "Hdfs writer",
      clazz = classOf[HdfsWriter].getName,
      inputs = Map (
        "In" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("stationid", "timestamp", "temperature")))
      ),
      properties = Map (
        "Location prefix" -> s"${workingDirectory.path}/sensor",
        "Location suffix" -> "-data",
        "Format" -> "Sequence file (Java serialization)"
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 8),
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:28", "temperature" -> 9),
      Instance("stationid" -> "1", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 19)
    )

    // Then
    eventually {
      writtenData(workingDirectory, javaDeserialization) contains only (
        Map("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 8),
        Map("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:28", "temperature" -> 9),
        Map("stationid" -> "1", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 19)
      )
    }
  }

  "Hdfs writer" should "write features to hdfs using JSON serialization" in {

    val workingDirectory = Directory.makeTemp("hdfs-write-spec")

    val configuration = ComponentConfiguration (
      name = "Hdfs writer",
      clazz = classOf[HdfsWriter].getName,
      inputs = Map (
        "In" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("stationid", "timestamp", "temperature")))
      ),
      properties = Map (
        "Location prefix" -> s"${workingDirectory.path}/sensor",
        "Location suffix" -> "-data",
        "Format" -> "Text file (JSON)"
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 8),
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:28", "temperature" -> 9),
      Instance("stationid" -> "1", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 19)
    )

    // Then
    eventually {
      writtenData(workingDirectory, jsonDeserialization) contains only (
        Map("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 8),
        Map("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:28", "temperature" -> 9),
        Map("stationid" -> "1", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 19)
      )
    }
  }

  "Hdfs writer" should "write features to hdfs using CSV serialization" in {

    val workingDirectory = Directory.makeTemp("hdfs-write-spec")

    val configuration = ComponentConfiguration (
      name = "Hdfs writer",
      clazz = classOf[HdfsWriter].getName,
      inputs = Map (
        "In" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("stationid", "timestamp", "temperature")))
      ),
      properties = Map (
        "Location prefix" -> s"${workingDirectory.path}/sensor",
        "Location suffix" -> "-data",
        "Format" -> "Text file (CSV)"
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 8),
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:28", "temperature" -> 9),
      Instance("stationid" -> "1", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 19)
    )

    // Then
    eventually {
      writtenData(workingDirectory, csvDeserialization) contains only (
        Map("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 8),
        Map("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:28", "temperature" -> 9),
        Map("stationid" -> "1", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 19)
      )
    }
  }

  private val mapper = new ObjectMapper()

  def javaDeserialization(line: String):  Map[String, Any] = SerializationUtils.deserialize(line.getBytes).asInstanceOf[Map[String, Any]]
  def jsonDeserialization(line: String):  Map[String, Any] = mapper.readValue(line, classOf[Map[String, Any]])
  def csvDeserialization(line: String):  Map[String, Any] = {
    val Array(stationId, timestamp, temperature) = line.split(",")
    Map("stationid" -> stationId, "timestamp" -> timestamp, "temperature" -> temperature.toInt)
  }

  def writtenData(directory: Directory, deserializer:(String) => Map[String, Any]): List[Map[String, Any]] = {
    directory.deepFiles
      .flatMap(file => Source.fromFile(file.path).getLines)
      .map(line => deserializer(line))
      .toList
  }
}
