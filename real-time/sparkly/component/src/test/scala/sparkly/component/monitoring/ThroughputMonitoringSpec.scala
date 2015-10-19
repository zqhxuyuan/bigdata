package sparkly.component.monitoring

import sparkly.core._
import sparkly.testing._

class ThroughputMonitoringSpec extends ComponentSpec {

  "ThroughputMonitoring" should "monitor stream's throughput" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Instance throughput", clazz = classOf[ThroughputMonitoring].getName,
      properties = Map("Window length (in ms)" -> "1000"),
      inputs = Map("In" -> StreamConfiguration()),
      monitorings = Map("Throughput" -> MonitoringConfiguration(active = true))
    )

    // When
    val component = deployComponent(configuration)

    val data = (1 to 100).map(i => Instance("index" -> i)).toList
    component.inputs("In").push(data)

    // Then
    eventually {
      val data = component.latestMonitoringData[Map[String, Double]]("Throughput").data
      data("Throughput") should be equals(100.0)
    }
  }

}
