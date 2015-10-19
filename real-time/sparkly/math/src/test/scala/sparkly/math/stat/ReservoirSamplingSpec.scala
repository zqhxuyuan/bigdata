package sparkly.math.stat

import org.scalameter.api._
import org.scalatest._

import scala.util.Random

class ReservoirSamplingSpec extends FlatSpec with Matchers {

  "ReservoirSampling" should "sample values" in {
    // Given
    val data1 = List.fill(10000)(Random.nextGaussian())
    val data2 = List.fill(10000)(Random.nextGaussian())
    val reservoir = ReservoirSampling[Double](1000)

    // When
    val samples = reservoir.addAll(data1).addAll(data2).samples

    // Then
    val count1 = data1.count(v => samples.contains(v))
    val count2 = data2.count(v => samples.contains(v))
    (count1 / (count1 + count2).toDouble) should be (0.5 +- 0.1)
  }

  "ReservoirSampling" should "support reduce" in {
    // Given
    val data1 = List.fill(7000)(Random.nextGaussian())
    val data2 = List.fill(3000)(Random.nextGaussian())
    val reservoir1 = ReservoirSampling[Double](1000, data1)
    val reservoir2 = ReservoirSampling[Double](1000, data2)

    // When
    val samples = (reservoir1 + reservoir2).samples

    // Then
    val count1 = data1.count(v => samples.contains(v))
    val count2 = data2.count(v => samples.contains(v))
    (count1 / (count1 + count2).toDouble) should be (0.7 +- 0.1)
  }
}


object ReservoirSamplingBench extends PerformanceTest.Quickbenchmark {

  val capacity = 50
  val partitions = 10

  val dataGen = Gen.single("data")(List.fill(10000)(Random.nextDouble))
  val reservoirGen = Gen.single("reservoir")(List.fill(partitions){
    val data = List.fill(10000)(Random.nextDouble)
    ReservoirSampling(capacity, data)
  })

  performance of "FeatureReservoirSampling" in {
    // measurements: 0.425507, 0.451111, 0.404156, 0.413151, 0.445353, 0.418444, 0.445366, 0.431274, 0.465378, 0.459399, 0.426756, 0.449691, 0.527619, 0.472142, 0.434212, 0.432865, 0.402935, 0.435475, 0.429743, 0.409546, 0.414596, 0.412554, 0.401955, 0.441769, 0.412475, 0.411328, 0.405458, 0.447573, 0.415466, 0.411464, 0.435468, 0.410033, 0.437568, 0.413425, 0.43529, 0.463571
    measure method "add" in {
      using(dataGen) in { data =>
        ReservoirSampling[Double](capacity).addAll(data)
      }
    }

    // measurements: 0.058293, 0.070765, 0.071833, 0.065043, 0.067811, 0.069371, 0.06848, 0.070949, 0.071017, 0.070979, 0.067752, 0.068386, 0.068796, 0.065064, 0.067523, 0.068693, 0.070795, 0.068852, 0.068625, 0.068705, 0.070535, 0.071203, 0.06692, 0.069831, 0.069261, 0.065202, 0.06779, 0.069223, 0.06794, 0.066533, 0.069776, 0.063935, 0.066753, 0.06546, 0.066699, 0.092637
    measure method "merge" in {
      using(reservoirGen) in { reservoirs =>
        reservoirs.reduce(_ + _)
      }
    }
  }
}