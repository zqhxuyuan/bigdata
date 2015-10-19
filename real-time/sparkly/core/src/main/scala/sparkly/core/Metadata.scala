package sparkly.core

import sparkly.core.FeatureType.FeatureType
import sparkly.core.PropertyType.PropertyType

case class ComponentMetadata (
  name: String,
  description: String = "", category: String = "Others",
  properties: Map[String, PropertyMetadata] = Map(),
  inputs: Map[String, InputStreamMetadata] = Map(),
  outputs: Map[String, OutputStreamMetadata] = Map(),
  monitorings: Map[String, MonitoringMetadata] = Map()
)

case class MonitoringMetadata (
  chartType: String,
  values: List[String] = List(),
  primaryValues: List[String] = List(),
  unit: String = ""
)

object ChartType {
  val LINES = "LINES"
  val AREAS = "AREAS"
  val STACKED_AREAS = "STACKED_AREAS"
}

case class PropertyMetadata (
  propertyType: PropertyType,
  defaultValue: Option[_] = None,
  acceptedValues: List[String] = List(),
  mandatory: Boolean = true,
  description: String = ""
)

object PropertyType extends Enumeration {
  type PropertyType = Value
  val DECIMAL, STRING, INTEGER, LONG, BOOLEAN  = Value
}

case class InputStreamMetadata (
  namedFeatures: Map[String, FeatureType] = Map(),
  listedFeatures: Map[String, FeatureType] = Map()
)

case class OutputStreamMetadata (
  from: Option[String] = None,
  namedFeatures: Map[String, FeatureType] = Map(),
  listedFeatures: Map[String, FeatureType] = Map()
)

object FeatureType extends Enumeration {
  type FeatureType = Value
  val CATEGORICAL, CONTINUOUS, STRING, DOUBLE, INTEGER, LONG, NUMBER, BOOLEAN , DATE, VECTOR, ANY = Value
}

object ComponentMetadata {
  def of(component: ComponentConfiguration): ComponentMetadata = {
    Class.forName(component.clazz).newInstance.asInstanceOf[Component].metadata
  }
}