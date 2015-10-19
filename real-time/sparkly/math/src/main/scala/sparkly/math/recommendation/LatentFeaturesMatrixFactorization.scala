package sparkly.math.recommendation

import org.apache.spark._
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.linalg.VectorUtil._
import org.apache.spark.rdd.RDD

class LatentFeaturesMatrixFactorization (options: LatentFeaturesMatrixFactorizationOptions) extends Serializable with Logging {

  def train(ratings: RDD[Rating[Long]]): LatentFeaturesMatrixFactorizationModel = {
    train(ratings, LatentFeaturesMatrixFactorizationModel.init(options, ratings.sparkContext))
  }

  def blockIndex(r: Rating[Long]): (Int, Int) = {
    ((r.user % options.k).toInt, (r.item % options.k).toInt)
  }

  def train(ratings: RDD[Rating[Long]], model: LatentFeaturesMatrixFactorizationModel): LatentFeaturesMatrixFactorizationModel = {
    val trainStart = System.currentTimeMillis

    val groupedRatings = ratings.groupBy(blockIndex _, new BlockPartitioner(options.k)).cache()
    var (userBlocks, itemBlocks) = (model.userBlocks, model.itemBlocks)

    for (i <- 0 until options.iterations) {
      val currentStepSize = options.stepSize * math.pow(options.stepDecay, i).toFloat
      for (strataIndex <- 0 until options.k) {
        // Create strata
        val strata = (0 until options.k).map( q => ((q + strataIndex) % options.k, q)).toMap
        val invertedStrata = strata.map(_.swap)

        // Group ratings and blocks together
        val groupedUserBlocks = userBlocks.map{case(p, block) => ((p, strata(p)), block)}
        val groupedItemBlocks = itemBlocks.map{case (q, block) => ((invertedStrata(q), q), block)}
        val strataBlocksAndRatings = groupedUserBlocks
          .cogroup(groupedItemBlocks, groupedRatings, new BlockPartitioner(options.k))
          .flatMapValues{ case( userBlockIt, itemBlockIt, blockRatingsIt) =>
            if(blockRatingsIt.nonEmpty) {
              for (userBlock <- userBlockIt.iterator; itemBlock <- itemBlockIt.iterator; blockRatings <- blockRatingsIt.iterator) yield (userBlock, itemBlock, blockRatings)
            } else {
              for (userBlock <- userBlockIt.iterator; itemBlock <- itemBlockIt.iterator) yield (userBlock, itemBlock, Iterable[Rating[Long]]())
            }
          }

        // Distributed DSGD
        val updatedBlocks = strataBlocksAndRatings.map{ case (blockPosition, (userBlock, itemBlock, blockRatings)) =>
          blockSgd(currentStepSize, userBlock, itemBlock, blockRatings)
          ((blockPosition._1, userBlock), (blockPosition._2, itemBlock))
        }.cache()

        userBlocks = updatedBlocks.map(_._1)
        itemBlocks = updatedBlocks.map(_._2)
      }
    }

    val counts = (userBlocks.cache().count(), itemBlocks.cache().count()) // Force computation
    logInfo(s"Model with ${counts._1 * counts._2} blocks updated in ${System.currentTimeMillis - trainStart}ms")
    LatentFeaturesMatrixFactorizationModel(options, userBlocks, itemBlocks, model.ratings + ratings.count)
  }

  private def blockSgd (
    stepSize: Float,
    userBlock: LatentFeaturesBlock,
    itemBlock: LatentFeaturesBlock,
    ratings: Iterable[Rating[Long]]
   ): Unit = {
    ratings.foreach { rating =>
      val userFeatures = userBlock.getFeatures(rating.user)
      val itemFeatures = itemBlock.getFeatures(rating.item)
      val ui = userFeatures.vector
      val vj = itemFeatures.vector

      val error = rating.rating - UserItemBlock(options, userBlock, itemBlock).predict(userFeatures, itemFeatures)

      if (Float.box(error).isNaN || Float.box(error).isInfinite) {
        // TODO : User/Item model diverged, Log it ? It can cause lot of logs !!
        logDebug(s"Model diverged (user ${rating.user}, item ${rating.item})")
      } else {
        val newUi = ui + (vj * error - ui * options.lambda) * stepSize
        val newUserBias = stepSize * (error - options.lambda * userFeatures.bias)
        val newVj = vj + (ui * error - vj * options.lambda) * stepSize
        val newItemBias = stepSize * (error - options.lambda * itemFeatures.bias)

        userBlock.setFeatures(rating.user, LatentFeatures(newUserBias, newUi))
        itemBlock.setFeatures(rating.item, LatentFeatures(newItemBias, newVj))
      }
    }

  }
}


case class BlockPartitioner(k: Int) extends Partitioner {

  def numPartitions: Int = k * k

  def getPartition(key: Any): Int = key match {
    case (p: Int, q: Int) => getPartition(p, q)
    case _ => throw new IllegalArgumentException(s"Unexpected key : $key")
  }

  private def getPartition(p: Int, q: Int) = p * k + q

}