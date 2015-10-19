package sparkly.component.regressor

import sparkly.math.regression.RunningRmsd

import scala.Some
import sparkly.core._
import breeze.linalg._
import breeze.stats.distributions.Rand
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

class PerceptronRegressor extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Perceptron regressor", category = "Regressor",
    description =
      """
        |Regressor based on an averaged kernel-based perceptron.
      """.stripMargin,
    inputs = Map (
      "Train" -> InputStreamMetadata(namedFeatures = Map("Label" -> FeatureType.DOUBLE, "Features" -> FeatureType.VECTOR)),
      "Predict" -> InputStreamMetadata(namedFeatures = Map("Features" -> FeatureType.VECTOR))
    ),outputs = Map (
      "Predictions" -> OutputStreamMetadata(from = Some("Predict"), namedFeatures = Map("Prediction" -> FeatureType.DOUBLE))
    ), properties = Map (
      "Bias" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(0.0)),
      "Learning rate" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(0.1))
    ),
    monitorings = Map("NRMSD" -> MonitoringMetadata(ChartType.LINES, values = List("NRMSD"), primaryValues = List("RMSD", "NRMSD")))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val bias = context.properties("Bias").as[Double]
    val learningRate = context.properties("Learning rate").as[Double]

    var model = PerceptronRegressorModel(bias, learningRate)
    val monitoring = context.createMonitoring[Map[String, Double]]("NRMSD")

    // Update model
    context.dstream("Train").map{ i =>
      val label = i.inputFeature("Label").asDouble
      val features = i.inputFeature("Features").asVector
      (label, features)
    }.foreachRDD{ (rdd, time) =>
      model = model.update(rdd)
      monitoring.add(time.milliseconds, Map("RMSD" -> model.rmse.value, "NRMSD" -> model.rmse.normalized))
    }


    // Predict
    val predictions = context.dstream("Predict", "Predictions").map{ i =>
      val features = i.inputFeature("Features").asVector
      val label = model.predict(features)
      i.outputFeature("Prediction", label)
    }

    Map("Predictions" -> predictions)
  }

}

object PerceptronRegressorModel {
  def apply(bias: Double, learningRate: Double) = new PerceptronRegressorModel(bias, learningRate, null, RunningRmsd())
}

case class PerceptronRegressorModel(bias: Double, learningRate: Double, weights: DenseVector[Double], rmse: RunningRmsd) {

  def update(rdd: RDD[(Double, Vector[Double])]): PerceptronRegressorModel = if(rdd.isEmpty) this else {
    var model = if(weights == null) this.init(rdd.first()._2) else this
    rdd.collect().foreach{ case (label, features) =>
      model = model.update(label, features)
    }
    model
  }

  def predict(features: Vector[Double]): Double = {
    val w = if(weights == null) initialWeights(features) else weights
    features.t * w + bias
  }

  private def update(label: Double, features: Vector[Double]): PerceptronRegressorModel = {
    val prediction = predict(features)
    val error = label - prediction
    val newWeights = weights + (features :* (learningRate * error))
    val newRmse = rmse.update(prediction, label)

    this.copy(weights = newWeights, rmse = newRmse)
  }

  private def init(features: Vector[Double]): PerceptronRegressorModel = {
    this.copy(weights = initialWeights(features))
  }

  private def initialWeights(features: Vector[Double]): DenseVector[Double] = {
    val featureCount = features.length
    DenseVector.rand[Double](featureCount, rand = Rand.gaussian) .* (1.0 / featureCount)
  }
}
