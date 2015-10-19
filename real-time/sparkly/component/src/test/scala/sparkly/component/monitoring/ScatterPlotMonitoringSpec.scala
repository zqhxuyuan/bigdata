package sparkly.component.monitoring

import sparkly.core._
import sparkly.math._
import sparkly.testing._

import scala.util.Random

class ScatterPlotMonitoringSpec extends ComponentSpec {

  "ScatterPlotMonitoring" should "create scatter plot" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "component", clazz = classOf[ScatterPlotMonitoring].getName,
      inputs = Map("In" -> StreamConfiguration(selectedFeatures = Map("Number features" -> List("age", "salary", "debt")))),
      monitorings = Map("Scatter plot" -> MonitoringConfiguration(active = true)),
      properties = Map ("Bins" -> "10", "Compression" -> "100")
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(1000, () => {
      val age = Math.abs(30 + 30 * Random.nextGaussian)
      val salary = Math.abs(1000 + 100 * Random.nextGaussian)
      val debt = Random.nextDouble * 10000
      Instance("age" -> age, "salary" -> salary, "debt" -> debt)
    })

    // Then
    eventually {
      val data = component.latestMonitoringData[ScatterPlotData]("Scatter plot").data
      data.histograms.map(hist => hist.feature) should contain only ("age", "salary", "debt")
      data.samples should not be empty
    }
  }

}
