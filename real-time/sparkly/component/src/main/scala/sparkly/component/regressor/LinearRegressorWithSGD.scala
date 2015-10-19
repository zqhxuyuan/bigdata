package sparkly.component.regressor

import org.apache.spark.mllib.regression.LabeledPoint
import sparkly.core._
import sparkly.math.regression._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.mllib.linalg.VectorUtil._
import sparkly.math.regression.StreamingLinearRegressorWithSGD

class LinearRegressorWithSGD extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Linear regressor", category = "Regressor",
    description =
      """
        |Regressor based on MLlib's linear regression model with SGD.
        |
      """.stripMargin,
    inputs = Map (
      "Train" -> InputStreamMetadata(namedFeatures = Map("Label" -> FeatureType.CONTINUOUS, "Features" -> FeatureType.VECTOR)),
      "Predict" -> InputStreamMetadata(namedFeatures = Map("Features" -> FeatureType.VECTOR))
    ),outputs = Map (
      "Predictions" -> OutputStreamMetadata(from = Some("Predict"), namedFeatures = Map("Prediction" -> FeatureType.CONTINUOUS))
    ), properties = Map (
      "Step size" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(0.1), description = "Step size for gradient descent"),
      "Iterations" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(2), description = "Number of iterations of gradient descent to run per batch"),
      "Batch fraction" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(1.0), description = "Fraction of each batch to use for updates")
    ),
    monitorings = Map("NRMSD" -> MonitoringMetadata(ChartType.LINES, values = List("NRMSD"), primaryValues = List("RMSD", "NRMSD")))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val stepSize = context.properties("Step size").as[Double]
    val iterations = context.properties("Iterations").as[Int]
    val miniBatchFraction = context.properties("Batch fraction").as[Double]

    val model = new StreamingLinearRegressorWithSGD()
      .setStepSize(stepSize)
      .setNumIterations(iterations)
      .setMiniBatchFraction(miniBatchFraction)

    val train = context.dstream("Train").map{ i =>
      val label = i.inputFeature("Label").asDouble
      val features = i.inputFeature("Features").asVector.toSpark
      LabeledPoint(label, features)
    }.cache()

    // Prequential-testing
    if(context.isActive("NRMSD")) {
      val monitoring = context.createMonitoring[Map[String, Double]]("NRMSD")
      var rmsd = RunningRmsd()

      train.foreachRDD{ (rdd, time) => if(!rdd.isEmpty && model.isInitiated){
        val actual = model.predict(rdd.map(_.features))
        val expected = rdd.map(_.label)
        rmsd = rmsd.update(actual, expected)
        monitoring.add(time.milliseconds, Map("RMSD" -> rmsd.value, "NRMSD" -> rmsd.normalized))
      }}
    }

    // Train
    model.trainOn(train)

    val predictions = context
      .dstream("Predict", "Predictions").map{ i =>
      val features = i.inputFeature("Features").asVector.toSpark
      val label = model.predict(features)
      i.outputFeature("Prediction", label)
    }

    Map("Predictions" -> predictions)
  }

}