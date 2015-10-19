package sparkly.component.misc

import sparkly.core._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.Context
import sparkly.core.ComponentMetadata
import sparkly.component.common.ThroughputMonitoringFactory

class BasicFilter extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata(
    name = "Basic Filter", description = "Keeps only instances that satisfy a predicate.", category = "Miscellaneous",
    properties = Map(
      "Operator" -> PropertyMetadata(PropertyType.STRING, defaultValue = Some("IS NULL"), acceptedValues = List("=", ">", ">=", "<", "<=", "!=", "IS NULL", "IS NOT NULL")),
      "Operand" -> PropertyMetadata(PropertyType.STRING)
    ),
    inputs = Map("In" -> InputStreamMetadata(namedFeatures = Map("Feature" -> FeatureType.STRING))),
    outputs = Map("Out" -> OutputStreamMetadata(from = Some("In"))),
    monitorings = Map (
      "Throughput (In)" -> MonitoringMetadata(ChartType.LINES, values = List("Throughput"), primaryValues = List("Throughput"), unit = "instance/s"),
      "Throughput (Out)" -> MonitoringMetadata(ChartType.LINES, values = List("Throughput"), primaryValues = List("Throughput"), unit = "instance/s")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val predicate = createPredicate(context.properties)

    val in = context.dstream("In")
    val out = in.filter(instance => predicate(instance.inputFeature("Feature")))

    ThroughputMonitoringFactory.create("Throughput (In)", in, context)
    ThroughputMonitoringFactory.create("Throughput (Out)", out, context)

    Map("Out" -> out)
  }

  def createPredicate(properties: Map[String, Property]): (Feature[_]) => Boolean = properties("Operator").as[String] match {
    case "IS NULL" => (f: Feature[_]) => f.isEmpty
    case "IS NOT NULL" => (f: Feature[_]) => f.isDefined
    case "=" => (f: Feature[_]) => f.asString == properties("Operand").as[String]
    case ">" => (f: Feature[_]) => f.asString > properties("Operand").as[String]
    case ">=" => (f: Feature[_]) => f.asString >= properties("Operand").as[String]
    case "<" => (f: Feature[_]) => f.asString < properties("Operand").as[String]
    case "<=" => (f: Feature[_]) => f.asString <= properties("Operand").as[String]
    case "!=" => (f: Feature[_]) => f.asString != properties("Operand").as[String]
  }
}