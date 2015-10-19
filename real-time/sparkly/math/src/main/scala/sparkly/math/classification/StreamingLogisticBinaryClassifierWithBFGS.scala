package org.apache.spark.mllib.classification

import org.apache.spark.mllib.regression._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.mllib.linalg._


class StreamingLogisticBinaryClassifierWithBFGS extends StreamingLinearAlgorithm[LogisticRegressionModel, LogisticRegressionWithLBFGS] with Serializable {

  protected var model: Option[LogisticRegressionModel] = None
  protected val algorithm = new LogisticRegressionWithLBFGS()
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
        case None => Vectors.zeros(rdd.first().features.size)
      }
      model = Some(algorithm.run(rdd, initialWeights))
    }}
  }

  def predict(features: Vector): Boolean = model match {
    case Some(m) => m.predict(features) > 0.5
    case None => false
  }

  def isInitiated: Boolean = model.isDefined
}

