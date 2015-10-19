package sparkly.math.recommendation

import org.apache.spark.Logging
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD
import org.scalatest._
import sparkly.math.testing.LocalSparkContext

import scala.io.Source

class LatentFeaturesMatrixFactorizationSpec extends FlatSpec with Matchers with LocalSparkContext with Logging {

  "LatentFeaturesMatrixFactorization" should "ok" in {
    val ks = List(2, 2)
    val ranks = List(10)
    val lambdas = List(0.1f)
    val stepSizes = List(0.1f)
    val stepDecays = List(0.7f)
    val iterationss = List(2, 5, 10)

    val train = sc.parallelize(fetch("http://files.grouplens.org/datasets/movielens/ml-100k/u1.base")).cache()
    val test = sc.parallelize(fetch("http://files.grouplens.org/datasets/movielens/ml-100k/u1.test")).map { rating =>
      (rating.rating, (rating.user, rating.item))
    }.cache()

    // Pre-load to avoid lazy loading that confuse benchs
    logInfo(s"Working with ${train.count} training and ${test.count} testing")

    var best: (Double, LatentFeaturesMatrixFactorizationOptions) = (Double.MaxValue, null)

    for(k <- ks; lambda <- lambdas; stepSize <- stepSizes; stepDecay <- stepDecays; iterations <- iterationss; rank <- ranks) {
      val options = LatentFeaturesMatrixFactorizationOptions (
        k = k,
        rank = rank,
        lambda = lambda,
        stepSize = stepSize,
        stepDecay = stepDecay,
        iterations = iterations
      )

      val start = System.currentTimeMillis()
      val rmse = run(options, train, test)
      val end = System.currentTimeMillis()
      println(s"$rmse for $options in ${end - start}ms")

      if(rmse < best._1)
        best = (rmse, options)
    }

    println(s"Best RMSE is ${best._1} with ${best._2}")
  }

  private def run(options: LatentFeaturesMatrixFactorizationOptions, train: RDD[Rating[Long]], test: RDD[(Float, (Long, Long))]): Double = {
    val mf = new LatentFeaturesMatrixFactorization(options)
    val model = mf.train(train)
    val predictions = model.predictValues(test)
    Math.sqrt(predictions.map{ case (e, a) => Math.pow(a - e, 2)}.mean())
  }

  private def fetch(url: String): Seq[Rating[Long]] = Source.fromURL(url).getLines().map{ line =>
    val values = line.split("\t")
    val user = values(0).toLong
    val item = values(1).toLong
    val rating = values(2).toFloat
    Rating[Long](user, item, rating)
  }.toSeq
}