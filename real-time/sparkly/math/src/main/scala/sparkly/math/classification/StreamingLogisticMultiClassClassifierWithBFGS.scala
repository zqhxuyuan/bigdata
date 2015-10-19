package org.apache.spark.mllib.classification

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.streaming.dstream.DStream


class StreamingLogisticMultiClassClassifierWithBFGS(numClasses: Int) extends StreamingLinearAlgorithm[LogisticRegressionModel, LogisticRegressionWithLBFGS] with Serializable {

  protected var model: Option[LogisticRegressionModel] = None
  protected val algorithm = new LogisticRegressionWithLBFGS()
  algorithm.setNumClasses(numClasses)
  algorithm.setFeatureScaling(false)

  def setNumCorrections(corrections: Int): this.type = {
    this.algorithm.optimizer.setNumCorrections(corrections)
    this
  }

  def setConvergenceTol(tolerance: Double): this.type = {
    this.algorithm.optimizer.setConvergenceTol(tolerance)
    this
  }
  def setNumIterations(numIterations: Int): this.type = {
    this.algorithm.optimizer.setNumIterations(numIterations)
    this
  }

  def setRegParam(regParam: Double): this.type = {
    this.algorithm.optimizer.setRegParam(regParam)
    this
  }

  override def trainOn(data: DStream[LabeledPoint]): Unit = {
    data.foreachRDD { (rdd, time) => if (!rdd.isEmpty) {
      val initialWeights = model match {
        case Some(m) => m.weights
        case None => Vectors.dense(new Array[Double](rdd.first().features.size * (numClasses - 1)))
      }
      model = Some(algorithm.run(rdd, initialWeights))
    }}
  }

  def predict(features: Vector): Int = model match {
    case Some(m) => m.predict(features).toInt
    case None => -1
  }

  def isInitiated: Boolean = model.isDefined
}
