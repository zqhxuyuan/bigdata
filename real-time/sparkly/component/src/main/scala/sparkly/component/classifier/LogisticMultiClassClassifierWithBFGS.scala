package sparkly.component.classifier

import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.linalg.VectorUtil._
import org.apache.spark.mllib.regression._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core._
import sparkly.math.classification.RunningAccuracy

class LogisticMultiClassClassifierWithBFGS extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Logistic multi-class classifier", category = "Classifier",
    description =
      """
        |Multi-class classifier based on MLlib's logistic regression model with BFGS.
      """.stripMargin,
    inputs = Map (
      "Train" -> InputStreamMetadata(namedFeatures = Map("Label" -> FeatureType.INTEGER, "Features" -> FeatureType.VECTOR)),
      "Predict" -> InputStreamMetadata(namedFeatures = Map("Features" -> FeatureType.VECTOR))
    ),outputs = Map (
      "Predictions" -> OutputStreamMetadata(from = Some("Predict"), namedFeatures = Map("Prediction" -> FeatureType.INTEGER))
    ), properties = Map (
      "Classes" -> PropertyMetadata(PropertyType.INTEGER, description = "Number of corrections used in the LBFGS update. 3 < numCorrections < 10 is recommended."),
      "Corrections" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(10), description = "Number of corrections used in the LBFGS update. 3 < numCorrections < 10 is recommended."),
      "Convergence tolerance" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(1E-3), description = "Convergence tolerance of iterations for L-BFGS. Smaller value will lead to higher accuracy with the cost of more iterations."),
      "Iterations" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(20), description = "Number of iterations of gradient descent to run per batch"),
      "Regularization" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(0.1), description = "Regularization parameter")
    ),
    monitorings = Map("Accuracy" -> MonitoringMetadata(ChartType.LINES, values = List("Accuracy"), primaryValues = List("Accuracy"), unit = "%"))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val numClasses = context.properties("Classes").as[Int]
    val corrections = context.properties("Corrections").as[Int]
    val convergenceTol = context.properties("Convergence tolerance").as[Double]
    val iterations = context.properties("Iterations").as[Int]
    val regularization = context.properties("Regularization").as[Double]

    val model = new StreamingLogisticMultiClassClassifierWithBFGS(numClasses)
      .setNumCorrections(corrections)
      .setConvergenceTol(convergenceTol)
      .setNumIterations(iterations)
      .setRegParam(regularization)

    var accuracy = RunningAccuracy[Double]()
    val monitoring = context.createMonitoring[Map[String, Double]]("Accuracy")

    val train = context.dstream("Train").map{ i =>
      val label = i.inputFeature("Label").asDouble
      val features = i.inputFeature("Features").asVector.toSpark
      LabeledPoint(label, features)
    }.cache()

    // Test
    train.foreachRDD{ (rdd, time) => if (!rdd.isEmpty && model.isInitiated) {
      val actual = model.latestModel().predict(rdd.map(_.features))
      val expected = rdd.map(_.label)
      accuracy = accuracy.update(expected, actual)
      monitoring.add(time.milliseconds, Map("Accuracy" -> accuracy.value * 100.0))
    }}

    // Then train
    model.trainOn(train)

    // Predict
    val predictions = context
      .dstream("Predict", "Predictions").map{ i =>
      val features = i.inputFeature("Features").asVector.toSpark
      val label = model.predict(features)
      i.outputFeature("Prediction", label)
    }

    Map("Predictions" -> predictions)
  }
}