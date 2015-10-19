package sparkly.core

import sparkly.core.SparkDefaultConfiguration._

import scala.util.Random

case class PipelineConfiguration (
  id: String = Random.alphanumeric.take(10).mkString,
  name: String, description: String = "",
  batchDurationMs: Long = defaultBatchDurationMs,
  components: List[ComponentConfiguration] = List(),
  connections: List[ConnectionConfiguration] = List(),
  settings: Map[String, Map[String, String]] = defaultSettings
)

case class ConnectionConfiguration(from: ConnectionPoint, to: ConnectionPoint) {
  def isTo(component: String, stream: String) = to == ConnectionPoint(component, stream)
  def isFrom(component: String, stream: String) = from == ConnectionPoint(component, stream)
}

case class ConnectionPoint(component: String, stream: String)

case class ComponentConfiguration (
  id: String = Random.alphanumeric.take(10).mkString,
  name: String, x: Int = 0, y: Int = 0,
  clazz: String,
  properties: Map[String, String] = Map(),
  inputs: Map[String, StreamConfiguration] = Map(),
  outputs: Map[String, StreamConfiguration] = Map(),
  monitorings: Map[String, MonitoringConfiguration] = Map()
) {
  def propertyExists(property: String): Boolean = {
    properties.contains(property) && properties(property) != null
  }
}

case class StreamConfiguration(mappedFeatures: Map[String, String] = Map(), selectedFeatures: Map[String, List[String]] = Map())

case class MonitoringConfiguration(active: Boolean = true)

object ConnectionConfiguration {
  def apply(fromComponent: String, fromStream: String, toComponent: String, toStream: String) = {
    new ConnectionConfiguration(ConnectionPoint(fromComponent, fromStream), ConnectionPoint(toComponent, toStream))
  }
}