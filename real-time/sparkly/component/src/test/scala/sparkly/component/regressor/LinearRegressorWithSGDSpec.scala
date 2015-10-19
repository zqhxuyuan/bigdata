package sparkly.component.regressor

import sparkly.testing._
import sparkly.core._
import sparkly.component.source.dataset.HousingDataset

class LinearRegressorWithSGDSpec extends ComponentSpec {

  "LogisticRegressorWithSGD" should "train on random data" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "LinearRegressorWithSGD",
      clazz = classOf[LinearRegressorWithSGD].getName,
      inputs = Map(
        "Train" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label", "Features" -> "Features")),
        "Predict" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Features"))
      ),
      outputs = Map("Predictions" -> StreamConfiguration(mappedFeatures = Map("Prediction" -> "Prediction"))),
      monitorings = Map("NRMSD" -> MonitoringConfiguration(active = true))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Train").push(500, DataGenerator.regressionData(10))
    component.inputs("Predict").push(500, DataGenerator.regressionData(10))

    // Then
    eventually {
      val monitoringData = component.latestMonitoringData[Map[String, Double]]("NRMSD")
      val nrmsd = monitoringData.data("NRMSD")
      nrmsd should be < 0.10
    }
  }


}
