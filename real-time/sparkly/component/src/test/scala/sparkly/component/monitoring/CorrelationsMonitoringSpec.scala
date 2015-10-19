package sparkly.component.monitoring

import sparkly.testing.ComponentSpec
import sparkly.core.{Instance, MonitoringConfiguration, StreamConfiguration, ComponentConfiguration}
import scala.util.Random

class CorrelationsMonitoringSpec extends ComponentSpec {

  "CorrelationsMonitoring" should "monitor correlations" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Correlations", clazz = classOf[CorrelationsMonitoring].getName,
      inputs = Map("In" -> StreamConfiguration(selectedFeatures = Map("Number features" -> List("age", "salary", "debt", "zipcode")))),
      monitorings = Map("Correlations" -> MonitoringConfiguration(active = true))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push(1000, () => {
      val age = Random.nextInt(100)
      val salary = if(age > 15 && age < 70) 0 else age * 10 + Random.nextDouble() * 100
      val debt = 50000 - age * 100000
      val zipcode = List(75020, 49100, 76000, 18420, 53750)(Random.nextInt(5))
      Instance("age" -> age, "salary" -> salary, "debt" -> debt, "zipcode" -> zipcode)
    })

    // Then
    eventually {
      val data = component.latestMonitoringData[Map[String, Double]]("Correlations").data
      data("age.age") should equal (1.0 +- 0.1)
      data("salary.salary") should equal (1.0 +- 0.1)
      data("debt.debt") should equal (1.0 +- 0.1)
      data("zipcode.zipcode") should equal (1.0 +- 0.1)

      data("age.salary") should be > 0.75
      data("age.debt") should be < -0.75

      Math.abs(data("age.zipcode")) should be < 0.1
    }
  }

}
