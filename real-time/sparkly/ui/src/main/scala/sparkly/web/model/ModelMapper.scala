package sparkly.web.model

import sparkly.core._
import sparkly.dao._
import sparkly.service._

class ModelMapper(implicit val componentRepository: ComponentRepository) {

  def convert(pipeline: PipelineConfiguration): PipelineConfigurationModel =  PipelineConfigurationModel (
    id = pipeline.id,
    name = pipeline.name,
    description = pipeline.description,
    batchDurationMs = pipeline.batchDurationMs,
    components = pipeline.components.map(convert),
    connections = pipeline.connections.map(convert),
    settings = pipeline.settings
  )

  def convert(component: ComponentConfiguration): ComponentConfigurationModel = ComponentConfigurationModel (
    id = component.id,
    name = component.name,
    x = component.x,
    y = component.y,
    metadata = convert(component.clazz, componentRepository.findByClassName(component.clazz)),
    properties = component.properties.map(d => convert(d._1, d._2)).toList,
    inputs = component.inputs.map(d => convert(d._1, d._2)).toList,
    outputs =  component.outputs.map(d => convert(d._1, d._2)).toList,
    monitorings = component.monitorings.map(d => convert(d._1, d._2)).toList
  )

  def convert(name: String, monitoring: MonitoringConfiguration): MonitoringConfigurationModel = MonitoringConfigurationModel (
    name = name,
    active = monitoring.active
  )

  def convert(id: String, metadata: ComponentMetadata): ComponentMetadataModel = ComponentMetadataModel (
    id = id,
    name = metadata.name,
    description = metadata.description,
    category = metadata.category,
    properties = metadata.properties.map(d => convert(d._1, d._2)).toList,
    inputs = metadata.inputs.map(d => convert(d._1, d._2)).toList,
    outputs = metadata.outputs.map(d => convert(d._1, d._2)).toList,
    monitorings = metadata.monitorings.map(d => convert(d._1, d._2)).toList
  )

  def convert(name: String, metadata: PropertyMetadata): PropertyMetadataModel = PropertyMetadataModel (
    name = name,
    propertyType = metadata.propertyType.toString,
    defaultValue = metadata.defaultValue,
    acceptedValues = metadata.acceptedValues,
    mandatory= metadata.mandatory,
    description = metadata.description
  )

  def convert(name: String, metadata: InputStreamMetadata): InputStreamMetadataModel = InputStreamMetadataModel (
    name = name,
    namedFeatures = metadata.namedFeatures.mapValues(_.toString()),
    listedFeatures = metadata.listedFeatures.mapValues(_.toString())
  )

  def convert(name: String, metadata: OutputStreamMetadata): OutputStreamMetadataModel = OutputStreamMetadataModel (
    name = name,
    from = metadata.from,
    namedFeatures = metadata.namedFeatures.mapValues(_.toString()),
    listedFeatures = metadata.listedFeatures.mapValues(_.toString())
  )

  def convert(name: String, metadata: MonitoringMetadata): MonitoringMetadataModel = MonitoringMetadataModel (
    name = name,
    chartType = metadata.chartType,
    values = metadata.values,
    primaryValues = metadata.primaryValues,
    unit = metadata.unit
  )

  def convert(name: String, value: String): PropertyConfigurationModel = PropertyConfigurationModel (
    name = name,
    value = value
  )

  def convert(name: String, configuration: StreamConfiguration): StreamConfigurationModel = StreamConfigurationModel (
    name = name,
    mappedFeatures =  configuration.mappedFeatures,
    selectedFeatures = configuration.selectedFeatures
  )

  def convert(connection: ConnectionConfiguration): ConnectionModel = ConnectionModel (
    from = ConnectionPointModel(connection.from.component, connection.from.stream),
    to = ConnectionPointModel(connection.to.component, connection.to.stream)
  )

  def convert(pipeline: PipelineConfigurationModel): PipelineConfiguration =  PipelineConfiguration (
    id = pipeline.id,
    name = pipeline.name,
    description = pipeline.description,
    batchDurationMs = pipeline.batchDurationMs,
    components = pipeline.components.map(convert),
    connections = pipeline.connections.map(convert),
    settings = pipeline.settings
  )

  def convert(component: ComponentConfigurationModel): ComponentConfiguration = ComponentConfiguration (
    id = component.id,
    name = component.name,
    x = component.x,
    y = component.y,
    clazz = component.metadata.id,
    properties = component.properties.map(convert).toMap,
    inputs = component.inputs.map(convert).toMap,
    outputs = component.outputs.map(convert).toMap,
    monitorings = component.monitorings.map(convert).toMap
  )

  def convert(monitoring: MonitoringConfigurationModel) = (monitoring.name, MonitoringConfiguration (
    active = monitoring.active
  ))

  def convert(property: PropertyConfigurationModel) = (property.name, property.value)

  def convert(input: StreamConfigurationModel) = (input.name, StreamConfiguration (
    mappedFeatures = input.mappedFeatures,
    selectedFeatures = input.selectedFeatures
  ))

  def convert(connection: ConnectionModel): ConnectionConfiguration = ConnectionConfiguration (
    from = ConnectionPoint(connection.from.component, connection.from.stream),
    to = ConnectionPoint(connection.to.component, connection.to.stream)
  )
  
  def convert(status: ClusterStatus): ClusterStatusModel = ClusterStatusModel (
    state = status.action.toString,
    time = status.time,
    pipeline = status.pipeline.map(pipeline => convert(pipeline))
  )

  def convert(report: ValidationReport): ValidationReportModel = ValidationReportModel (
    messages = report.messages.map(message => convert(message))
  )

  def convert(message: ValidationMessage): ValidationMessageModel = ValidationMessageModel (
    text = message.text,
    level = message.level.toString
  )
}
