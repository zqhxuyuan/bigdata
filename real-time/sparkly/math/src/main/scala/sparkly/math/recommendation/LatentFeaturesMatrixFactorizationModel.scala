package sparkly.math.recommendation

import org.apache.spark.mllib.linalg.VectorUtil._

import scala.collection.mutable.HashMap
import scala.util.Random

import org.apache.spark._
import org.apache.spark.rdd.RDD

object LatentFeaturesMatrixFactorizationModel {
  def init(options: LatentFeaturesMatrixFactorizationOptions, sc: SparkContext): LatentFeaturesMatrixFactorizationModel = {
    val userBlocks = (0 until options.k).map(index => (index, LatentFeaturesBlock(options, new HashMap[Long, LatentFeatures]())))
    val itemBlocks = (0 until options.k).map(index => (index, LatentFeaturesBlock(options, new HashMap[Long, LatentFeatures]())))


    LatentFeaturesMatrixFactorizationModel(options, sc.parallelize(userBlocks), sc.parallelize(itemBlocks), 0L)
  }
}

case class LatentFeaturesMatrixFactorizationModel (
  options: LatentFeaturesMatrixFactorizationOptions,
  userBlocks: RDD[(Int, LatentFeaturesBlock)],
  itemBlocks: RDD[(Int, LatentFeaturesBlock)],
  ratings: Long) {

  def predict(rdd: RDD[(Long, Long)]): RDD[Float] = {
    rdd
      .groupBy{r => ((r._1 % options.k).toInt, (r._2 % options.k).toInt)}
      .join(blocks)
      .flatMap { case (blockPosition, (userItems, (userBlock, itemBlock))) =>
        val block = UserItemBlock(options, userBlock, itemBlock)
        userItems.map { case (user, item) => block.predict(user, item)}
      }
  }

  def predictValues[V](rdd: RDD[(V, (Long, Long))]): RDD[(V, Float)] = {
      rdd
        .groupBy{ case (value, (user, item)) => ((user % options.k).toInt, (item % options.k).toInt)}
        .join(blocks)
        .flatMap{ case (blockPosition, (it, (userBlock, itemBlock))) =>
          val block = UserItemBlock(options, userBlock, itemBlock)
          it.map{ case (value, (user, item)) => (value, block.predict(user, item))}
        }
  }


  private def blocks(): RDD[((Int, Int), (LatentFeaturesBlock, LatentFeaturesBlock))] = {
    userBlocks.cartesian(itemBlocks).map{case ((userBlockIndex, userBlock), (itemBlockIndex, itemBlock)) => ((userBlockIndex, itemBlockIndex), (userBlock, itemBlock))}
  }

}

case class UserItemBlock(options: LatentFeaturesMatrixFactorizationOptions, userBlock: LatentFeaturesBlock, itemBlock: LatentFeaturesBlock) {

  private val scale = options.maxRating - options.minRating

  def predict(user: Long, item: Long): Float = {
    (userBlock.features.get(user), itemBlock.features.get(item)) match { // Could do better with only one of the 2
      case (Some(userFeatures), Some(itemFeatures)) => predict(userFeatures, itemFeatures)
      case _ => options.minRating + (scale / 2.0f)
    }
  }

  def predict(userFeatures: LatentFeatures, itemFeatures: LatentFeatures): Float = {
    options.minRating + (((userFeatures.vector dot itemFeatures.vector) + userFeatures.bias + itemFeatures.bias) * scale) match {
      case tooBig if tooBig > options.maxRating => options.maxRating
      case tooSmall if tooSmall < options.minRating => options.minRating
      case _ @ good => good
    }
  }
}

case class LatentFeaturesBlock (options: LatentFeaturesMatrixFactorizationOptions, features: HashMap[Long, LatentFeatures]) {

  def getFeatures(id: Long): LatentFeatures = features.getOrElse(id, randomFeatures())
  def setFeatures(id: Long, newFeatures: LatentFeatures) = features.put(id, newFeatures)

  private def randomFeatures() = LatentFeatures(Random.nextFloat(), Array.fill(options.rank)(Random.nextFloat / options.rank.toFloat))

}

case class LatentFeatures(bias: Float, vector: Array[Float])

case class LatentFeaturesMatrixFactorizationOptions (
    k: Int = 2,
    rank: Int = 10,
    lambda: Float = 0.01f,
    minRating: Float = 1.0f,
    maxRating: Float = 5.0f,
    stepSize: Float = 0.1f,
    stepDecay: Float = 0.7f,
    iterations: Int = 5
)