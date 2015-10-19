package sparkly.math.stat

import breeze.linalg._
import org.scalatest._

import scala.util.Random

class PearsonCorrelationsSpec extends FlatSpec with Matchers {

  "PearsonCorrelations" should "compute correlations" in {
    // Given
    val data = (1 to 1000).map(i => generateFeatures(i))

    // When
    val correlations = PearsonCorrelations(data.iterator).correlations()

    // Then
    correlations(0, 0) should be (1.0 +- 0.1)
    correlations(0, 1) should be (1.0 +- 0.1)
    correlations(0, 2) should be (0.0 +- 0.1)
    correlations(0, 3) should be (0.0 +- 0.1)

    correlations(1, 0) should be (1.0 +- 0.1)
    correlations(1, 1) should be (1.0 +- 0.1)
    correlations(1, 2) should be (0.0 +- 0.1)
    correlations(1, 3) should be (0.0 +- 0.1)

    correlations(2, 0) should be (0.0 +- 0.1)
    correlations(2, 1) should be (0.0 +- 0.1)
    correlations(2, 2) should be (1.0 +- 0.1)
    correlations(2, 3) should be (-1.0 +- 0.1)

    correlations(3, 0) should be (0.0 +- 0.1)
    correlations(3, 1) should be (0.0 +- 0.1)
    correlations(3, 2) should be (-1.0 +- 0.1)
    correlations(3, 3) should be (1.0 +- 0.1)
  }

  def generateFeatures(i: Int): DenseVector[Double] = {
    val f1 = i.toDouble
    val f2 = 2 * f1 + 3
    val f3 = Random.nextDouble() * 10
    val f4 = -4 * f3 + Random.nextDouble()
    DenseVector(f1, f2, f3, f4)
  }
}
