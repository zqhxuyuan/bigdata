package sparkly.component.classifier

import sparkly.component.source.dataset.OptDigitsDataset
import sparkly.core._
import sparkly.testing.ComponentSpec

class LogisticMultiClassClassifierWithBFGSSpec extends ComponentSpec {

  "LogisticMultiClassClassifierWithBFGS" should "train on digits data" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "LogisticMultiClassClassifierWithBFGS",
      clazz = classOf[LogisticMultiClassClassifierWithBFGS].getName,
      inputs = Map(
        "Train" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label", "Features" -> "Features")),
        "Predict" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Features"))
      ),
      outputs = Map("Predictions" -> StreamConfiguration(mappedFeatures = Map("Label" -> "prediction"))),
      properties = Map (
        "Classes" -> "10"
      ),
      monitorings = Map("Accuracy" -> MonitoringConfiguration(active = true))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Train").push(2000, OptDigitsDataset.iterator())

    // Then
    eventually {
      val monitoringData = component.latestMonitoringData[Map[String, Double]]("Accuracy")
      val accuracy = monitoringData.data("Accuracy")
      accuracy should be > 80.0
    }
  }

}
