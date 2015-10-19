package sparkly.testing

import sparkly.core._
import breeze.linalg.DenseVector
import scala.util.Random

object DataGenerator {

  def regressionData(featureSize: Int): Iterator[Instance] = {
    val multipliers = Array.fill(featureSize)(Random.nextDouble * (1 + Random.nextInt(2)))
    Iterator.continually {
      val features = Array.fill(featureSize)(Random.nextDouble * (if(Random.nextGaussian > 0.0) 1.0 else -1.0))
      val label = (0 until featureSize).map(i => multipliers(i) * features(i)).sum
      Instance("Label" -> label, "Features" -> DenseVector(features))
    }
  }
}
