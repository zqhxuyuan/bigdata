package sparkly.component.classifier

import sparkly.testing._
import sparkly.core._

import scala.io.Source
import sparkly.component.source.dataset.SpamDataset

class LogisticBinaryClassifierWithBFGSSpec extends ComponentSpec {

  "LogisticBinaryClassifierWithBFGS" should "train on spam data" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "LogisticBinaryClassifierWithBFGS",
      clazz = classOf[LogisticBinaryClassifierWithBFGS].getName,
      inputs = Map(
        "Train" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label", "Features" -> "Features")),
        "Predict" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Features"))
      ),
      outputs = Map("Predictions" -> StreamConfiguration(mappedFeatures = Map("Label" -> "prediction"))),
      properties = Map (
        "Iterations" -> "2"
      ),
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
