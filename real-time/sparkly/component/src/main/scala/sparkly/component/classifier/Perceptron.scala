package sparkly.component.classifier

import sparkly.math.classification.RunningAccuracy

import scala.Some

import sparkly.core._
import breeze.linalg._

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

class Perceptron extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Perceptron classifier", category = "Classifier",
    description =
      """
        |Binary classifier based on an averaged kernel-based perceptron.
      """.stripMargin,
    inputs = Map (
      "Train" -> InputStreamMetadata(namedFeatures = Map("Label" -> FeatureType.BOOLEAN, "Features" -> FeatureType.VECTOR)),
      "Predict" -> InputStreamMetadata(namedFeatures = Map("Features" -> FeatureType.VECTOR))
    ),outputs = Map (
      "Predictions" -> OutputStreamMetadata(from = Some("Predict"), namedFeatures = Map("Label" -> FeatureType.BOOLEAN))
    ), properties = Map (
      "Bias" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(0.0)),
      "Threshold" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(0.5)),
      "Learning rate" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(0.1))
    ), monitorings = Map("Accuracy" -> MonitoringMetadata(ChartType.LINES, values = List("Accuracy"), primaryValues = List("Accuracy"), unit = "%"))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val accuracyMonitoring = context.createMonitoring[Map[String, Double]]("Accuracy")
    val bias = context.properties("Bias").as[Double]
    val threshold = context.properties("Threshold").as[Double]
    val learningRate = context.properties("Learning rate").as[Double]

    var model = PerceptronModel(bias, threshold, learningRate, RunningAccuracy[Boolean]())

    // Update model
    context.dstream("Train").map{ i =>
      val label = i.inputFeature("Label").asBoolean
      val features = i.inputFeature("Features").asVector
      (label, features)
    }.foreachRDD{ (rdd, time) =>
        model = model.update(rdd)
        accuracyMonitoring.add(time.milliseconds, Map("Accuracy" -> model.accuracy.value * 100.0))
    }


    // Predict
    val predictions = context.dstream("Predict", "Predictions").map{ i =>
      val features = i.inputFeature("Features").asVector
      val label = model.predict(features)
      i.outputFeature("Label", label)
    }

    Map("Predictions" -> predictions)
  }
}

case class PerceptronModel(bias: Double, threshold: Double, learningRate: Double, accuracy: RunningAccuracy[Boolean], weights: Vector[Double] = null) {

  def update(rdd: RDD[(Boolean, Vector[Double])]): PerceptronModel = if(rdd.isEmpty) this else {
    var model = if(weights == null) this.init(rdd.first()._2) else this
    rdd.collect().foreach{ case (label, features) =>
      model = model.update(label, features)
    }
    model
  }

  def predict(features: Vector[Double]): Boolean = {
    val w = if(weights == null) initialWeights(features) else weights
    features.t * w + bias > 0
  }

  private def update(label: Boolean, features: Vector[Double]): PerceptronModel = {
    val prediction = predict(features)
    val newWeights = if( prediction != label) {
      val correction = learningRate * (if (label) 1.0 else -1.0)
      weights + (features :* correction)
    } else {
      weights
    }

    val newAccuracy = accuracy.update(prediction, label)
    this.copy(weights = newWeights, accuracy = newAccuracy)
  }

  private def init(features: Vector[Double]): PerceptronModel = {
    this.copy(weights = initialWeights(features))
  }

  private def initialWeights(features: Vector[Double]): Vector[Double] = {
    val featureCount = features.length
    DenseVector.rand[Double](featureCount) .* (1.0 / featureCount)
  }
}
