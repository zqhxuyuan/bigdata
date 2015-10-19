package sparkly.it

import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, Matchers}
import sparkly.component.classifier._
import sparkly.component.preprocess._
import sparkly.core._

import scala.reflect.io.Directory
import sparkly.component.source.dataset.SpamDataset
import org.apache.spark.metrics.sink.MonitoringTestingData

class PipelineIT extends FlatSpec with Matchers with Eventually {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(20, org.scalatest.time.Seconds)), interval = scaled(Span(100, Millis)))

  val pipelineDirectory = Directory.makeTemp()
  val streamingContextFactory = new StreamingContextFactory(pipelineDirectory.toString, "local[8]", "test-cluster")

  "Pipeline" should "build and connect components together" in {
    val pipelineConfig = PipelineConfiguration (
      name = "test",
      components = List (
        ComponentConfiguration (
          id = "spam_source",
          name = "Train data",
          clazz = classOf[SpamDataset].getName,
          properties = Map ("Throughput (instance/second)" -> "5000"),
          outputs = Map (
            "Instances" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label", "Features" -> "Features"))
          )
        ),
        ComponentConfiguration (
          id = "normalizer",
          name = "Normalizer",
          clazz = classOf[Normalizer].getName,
          inputs = Map ("Input" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Features"))),
          outputs = Map ("Output" -> StreamConfiguration(mappedFeatures = Map("Normalized features" -> "Normalized features")))
        ),
        ComponentConfiguration (
          id = "perceptron",
          name = "Learner",
          clazz = classOf[Perceptron].getName,
          inputs = Map (
            "Train" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label", "Features" -> "Normalized features")),
            "Predict" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Normalized features"))
          ),
          outputs = Map (
            "Predictions" -> StreamConfiguration(mappedFeatures = Map("Label" -> "prediction"))
          ),
          monitorings = Map("Accuracy" -> MonitoringConfiguration(active = true))
        )
      ),
      connections = List (
        ConnectionConfiguration("spam_source", "Instances", "normalizer", "Input"),
        ConnectionConfiguration("normalizer", "Output", "perceptron", "Train")
      ),
      batchDurationMs = 200
    )

    val (ssc, _) = streamingContextFactory.createStreamingContext(pipelineConfig)

    try {
      ssc.start()

      eventually {
        val accuracy = MonitoringTestingData.latest[Map[String, Double]]("perceptron-accuracy").data("Accuracy")
        accuracy should be > 0.80
      }

    } finally {
      ssc.stop()
    }

  }
}
