package sparkly.component.classifier

import sparkly.testing._
import sparkly.core._

import sparkly.component.source.dataset.SpamDataset

class PerceptronSpec extends ComponentSpec {

  "Perceptron" should "train on spam data" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Perceptron",
      clazz = classOf[Perceptron].getName,
      inputs = Map(
        "Train" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label", "Features" -> "Features")),
        "Predict" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Features"))
      ),
      outputs = Map("Predictions" -> StreamConfiguration(mappedFeatures = Map("Label" -> "prediction"))),
      monitorings = Map("Accuracy" -> MonitoringConfiguration(active = true))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Train").push(2000, SpamDataset.iterator())

    // Then
    eventually {
      val monitoringData = component.latestMonitoringData[Map[String, Double]]("Accuracy")
      val accuracy = monitoringData.data("Accuracy")
      accuracy should be > 80.0
    }
  }


}
