package sparkly.core

import org.scalatest._

class MonitoringSpec extends FlatSpec with Matchers {

  "Monitoring" should "support new data" in {
    // Given
    val monitoring = new Monitoring[Map[String, Double]]("test")
    val gauge = monitoring.createMetric()

    // When
    monitoring.add(Map("value1" -> 1.0, "value2" -> 2.0))
    monitoring.add(Map("value1" -> 1.1, "value2" -> 2.2))

    // Then
    val data = gauge.getValue.get.data
    data should contain only (
      "value1" -> 1.1, "value2" -> 2.2
    )

    gauge.getValue should not be defined
  }
}
