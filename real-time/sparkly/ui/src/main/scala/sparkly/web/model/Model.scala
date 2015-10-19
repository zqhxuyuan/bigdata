package sparkly.web.model

import java.util.Date

case class PipelineConfigurationModel (
  id: String,
  name: String,
  description: String,
  batchDurationMs: Long,
  components: List[ComponentConfigurationModel] = List(),
  connections: List[ConnectionModel] = List(),
  settings: Map[String, Map[String, String]] = Map()
)

case class ConnectionModel(from: ConnectionPointModel, to: ConnectionPointModel)
case class ConnectionPointModel(component: String, stream: String)

case class ComponentConfigurationModel (
  id: String,
  name: String, x: Int = 0, y: Int = 0,
  metadata: ComponentMetadataModel,
  properties: List[PropertyConfigurationModel] = List(),
  inputs: List[StreamConfigurationModel] = List(),
  outputs: List[StreamConfigurationModel] = List(),
  monitorings: List[MonitoringConfigurationModel] = List()
)

case class StreamConfigurationModel (
  name: String,
  mappedFeatures: Map[String, String] = Map(),
  selectedFeatures: Map[String, List[String]] = Map()
)


case class ComponentMetadataModel (
  id: String,
  name: String,
  description: String = "", category: String = "Others",
  properties: List[PropertyMetadataModel] = List(),
  inputs: List[InputStreamMetadataModel] = List(),
  outputs: List[OutputStreamMetadataModel] = List(),
  monitorings: List[MonitoringMetadataModel] = List()
)

case class PropertyMetadataModel (
  name: String,
  propertyType: String,
  defaultValue: Option[_] = None,
  acceptedValues: List[String] = List(),
  mandatory: Boolean = true,
  description: String = ""
)

case class InputStreamMetadataModel(name: String, namedFeatures: Map[String, String] = Map(),listedFeatures: Map[String, String] = Map())

case class OutputStreamMetadataModel(name: String, from: Option[String] = None, namedFeatures: Map[String, String] = Map(), listedFeatures: Map[String, String] = Map())

case class MonitoringMetadataModel(name: String, chartType: String, values: List[String], primaryValues: List[String], unit: String)

case class PropertyConfigurationModel(name: String, value: String)

case class MonitoringConfigurationModel(name: String, active: Boolean)

case class ClusterModel(id: String, name: String, status: ClusterStatusModel)
case class ClusterStatusModel(state: String, time: Option[Date], pipeline: Option[PipelineConfigurationModel])

case class ValidationReportModel(messages: List[ValidationMessageModel] = List())
case class ValidationMessageModel(text: String, level: String)

case class StreamIdentifierModel(name: String, component: String, stream: String)
case class FeatureIdentifierModel(name: String, component: String, stream: String, feature: String)