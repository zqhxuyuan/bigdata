package sparkly.component.monitoring

import sparkly.core._
import sparkly.testing._

class FeatureStatisticsMonitoringSpec extends ComponentSpec {

  "FeatureStatisticsMonitoring" should "monitor distinct feature's statistics" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Feature statistics", clazz = classOf[FeatureStatisticsMonitoring].getName,
      properties = Map("Window length (in ms)" -> "1000"),
      inputs = Map("In" -> StreamConfiguration(mappedFeatures = Map("Number feature" -> "value"))),
      monitorings = Map("Feature statistics" -> MonitoringConfiguration(active = true))
    )

    // When
    val component = deployComponent(configuration)

    val data = List(-10, 15, null, -5, 20, 25, -10, 15, 10, null).map(value => Instance("value" -> value)).toList
    component.inputs("In").push(data)

    // Then
    eventually {
      val data = component.latestMonitoringData[Map[String, Double]]("Feature statistics").data
      data("count") should equal (10.0)
      data("missing") should equal (2.0)
      data("min") should equal (-10.0)
      data("max") should equal (25.0)
      data("mean") should equal (7.5 +- 1)
      data("std") should equal (12.5 +- 1)
      data("quantile 0.25") should equal (-10.0 +- 1)
      data("quantile 0.50") should equal (17.5 +- 1)
      data("quantile 0.75") should equal (15.0 +- 1)
      data("quantile 0.90") should equal (27.5 +- 1)
      data("quantile 0.99") should equal (27.5 +- 1)
    }
  }

}
