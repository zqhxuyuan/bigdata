package sparkly.math.recommendation

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark._

class StreamingLatentFeaturesMatrixFactorization(options: LatentFeaturesMatrixFactorizationOptions) extends Serializable with Logging {

  private val optimizer = new LatentFeaturesMatrixFactorization(options)
  private var model: Option[LatentFeaturesMatrixFactorizationModel] = None

  def trainOn(ratings: DStream[Rating[Long]]): Unit = {
    ratings.foreachRDD { (rdd, time) => if(!rdd.isEmpty) {
      model = Some(optimizer.train(rdd, latestModel(rdd.sparkContext)))
    }}
  }

  def predictOn(predict: DStream[(Long, Long)]): DStream[Float] = {
    predict.transform{ rdd =>
      if(!rdd.isEmpty) {
        latestModel(rdd.sparkContext).predict(rdd)
      } else {
        rdd.sparkContext.emptyRDD[Float]
      }
    }
  }

  def predictOnValues[V](predict: DStream[(V, (Long, Long))]): DStream[(V, Float)] = {
    predict.transform { rdd =>
      if(!rdd.isEmpty) {
        val lastModel = latestModel(rdd.sparkContext)
        lastModel.predictValues(rdd)
      } else {
        rdd.sparkContext.emptyRDD[(V, Float)]
      }
    }
  }

  def latestModel(sc: SparkContext) = model match {
    case Some(existing) => existing
    case None => LatentFeaturesMatrixFactorizationModel.init(options, sc)
  }

  def isInitiated = model.isDefined
}
