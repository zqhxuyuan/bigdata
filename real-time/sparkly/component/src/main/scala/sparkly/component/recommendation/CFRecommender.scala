package sparkly.component.recommendation

import org.apache.spark.streaming.dstream.DStream
import sparkly.math.regression.RunningRmsd
import scala.Some

import sparkly.core._
import sparkly.core.PropertyType._
import sparkly.math.recommendation._
import org.apache.spark.ml.recommendation.ALS.Rating

class CFRecommender extends Component {

  def metadata = ComponentMetadata (
    name = "CF Recommender", category = "Recommendation",
    description =
      """
        |Recommendation system based on collaborative filtering.
      """.stripMargin,
    inputs = Map (
      "Ratings" -> InputStreamMetadata(namedFeatures = Map("User" -> FeatureType.LONG, "Item" -> FeatureType.LONG, "Rating" -> FeatureType.DOUBLE)),
      "Predict" -> InputStreamMetadata(namedFeatures = Map("User" -> FeatureType.LONG, "Item" -> FeatureType.LONG))
    ),
    outputs = Map (
      "Predictions" -> OutputStreamMetadata(from = Some("Predict"), namedFeatures = Map("Prediction" -> FeatureType.DOUBLE))
    ),
    properties = Map(
      "K" -> PropertyMetadata(INTEGER, defaultValue = Some(2), description = "Computing parallelism."),
      "Rank" -> PropertyMetadata(INTEGER, defaultValue = Some(10), description = "Number of latent features per user/item"),
      "Lambda" -> PropertyMetadata(DECIMAL, defaultValue = Some(0.01), description = "Regularization parameter"),
      "Step size" -> PropertyMetadata(DECIMAL, defaultValue = Some(0.1), description = "Initial step size of SGD"),
      "Step decay" -> PropertyMetadata(DECIMAL, defaultValue = Some(0.7)),
      "Minimum rating" -> PropertyMetadata(DECIMAL, defaultValue = Some(0.0), description = "Minimum rating expected"),
      "Maximum rating" -> PropertyMetadata(DECIMAL, defaultValue = Some(5.0), description = "Maximum rating expected"),
      "Iterations" -> PropertyMetadata(INTEGER, defaultValue = Some(10), description = "Number of iterations for SGD")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val options = LatentFeaturesMatrixFactorizationOptions(
      k  = context.property("K").as[Int],
      rank = context.property("Rank").as[Int],
      lambda = context.property("Lambda").as[Double].toFloat,
      minRating = context.property("Minimum rating").as[Double].toFloat,
      maxRating = context.property("Maximum rating").as[Double].toFloat,
      stepSize = context.property("Step size").as[Double].toFloat,
      stepDecay = context.property("Step decay").as[Double].toFloat,
      iterations = context.property("Iterations").as[Int]
    )

    val model = new StreamingLatentFeaturesMatrixFactorization(options)

    val ratings = context.dstream("Ratings").map { i =>
      val user = i.inputFeature("User").asInt
      val item = i.inputFeature("Item").asInt
      val rating = i.inputFeature("Rating").asDouble.toFloat
      Rating[Long](user, item, rating)
    }.cache()

    // Train
    model.trainOn(ratings)

    // Predict
    val predict = context.dstream("Predict", "Predictions").map(i => (i, (i.inputFeature("User").asLong, i.inputFeature("Item").asLong)))
    val predictions = model.predictOnValues[Instance](predict).map{ case(instance, prediction) =>
      instance.outputFeature("Prediction", prediction.toDouble)
    }

    Map("Predictions" -> predictions)
  }

}
