package sparkly.math.regression

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.util.Random

class StreamingLinearRegressorWithSGD extends org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD {

  override def trainOn(data: DStream[LabeledPoint]): Unit = {
    data.foreachRDD { (rdd, time) => if (!rdd.isEmpty) {
      val weights = model match {
        case Some(m) => m.weights
        case None => Vectors.dense(Array.fill(rdd.first.features.size)(Random.nextDouble))
      }
      model = Some(algorithm.run(rdd, weights))
    }}
  }

  def predict(features: Vector): Double = model match {
    case Some(m) => m.predict(features)
    case None => 0.0
  }

  def predict(rdd: RDD[Vector]): RDD[Double] = latestModel().predict(rdd)

  def isInitiated: Boolean = model.isDefined
}