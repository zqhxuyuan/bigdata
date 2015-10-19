package sparkly.math.feature

import breeze.linalg.DenseVector
import org.scalameter.api._
import org.scalatest._

import scala.util.Random

class StandardizerSpec extends FlatSpec with Matchers {

  "Standardizer" should "standardize vectors" in {
    // Given
    val size = 1000
    val features = 5

    val originalValues = (1 to size).map{i =>
      val data = (1 to features).map(j => j * Random.nextGaussian + (j / 2)).toArray
      DenseVector(data)
    }

    // When
    val standardizer = Standardizer(features, originalValues.iterator)

    val standardizedValues = originalValues.map{ vector =>
      standardizer.standardize(vector)
    }

    // Then
    (0 until features).foreach{ i =>
      val values = standardizedValues.map(_(i)).toList
      (values.sum / values.size) should be (0.0 +- 0.1)
      mean(values) should be (0.0 +- 0.1)
      stddev(values) should be (1.0 +- 0.1)
    }
  }

  "Standardizer" should "support zeros" in {
    // Given
    val size = 1000
    val features = 5

    val originalValues = (1 to size).map{i => DenseVector.fill(features, 0.0)}


    // When
    val standardizer = Standardizer(features, originalValues.iterator)

    val standardizedValues = originalValues.map{ vector =>
      standardizer.standardize(vector)
    }

    // Then
    standardizedValues.foreach{ values =>
      values should be (DenseVector.fill(features, 0.0))
    }
  }

  "Standardizer" should "support merge" in {
    // Given
    val size = 1000
    val features = 5

    val (originalValues1, originalValues2) = (1 to size).map{i =>
      val data = (1 to features).map(j => j * Random.nextGaussian + (j / 2)).toArray
      DenseVector(data)
    }.splitAt(size / 2)

    val standardizer1 = Standardizer(features, originalValues1.iterator)
    val standardizer2 = Standardizer(features, originalValues2.iterator)

    // When
    val standardizer = standardizer1 + standardizer2
    val standardizedValues = originalValues1.map(vector => standardizer.standardize(vector)) ++ originalValues2.map(vector => standardizer.standardize(vector))

    // Then
    (0 until features).foreach{ i =>
      val values = standardizedValues.map(_(i)).toList
      (values.sum / values.size) should be (0.0 +- 0.1)
      mean(values) should be (0.0 +- 0.1)
      stddev(values) should be (1.0 +- 0.1)
    }
  }


  def stddev(values: Iterable[Double]): Double = {
    val mean = values.sum / values.size
    val devs = values.map(v => (v - mean) * (v - mean))
    Math.sqrt(devs.sum / values.size)
  }

  def mean(values: Iterable[Double]): Double = values.sum / values.size
}

object StandardizerBench extends PerformanceTest.Quickbenchmark {

  val features = 10
  val size = 1000
  val partitions = 10

  val dataGen = Gen.single("data")(List.fill(size)(DenseVector.rand[Double](features)))
  val standardizersGen = Gen.single("standarizers")(List.fill(partitions){
    val data = List.fill(size)(DenseVector.rand[Double](features))
    Standardizer(features, data.iterator)
  })

  performance of "Standardizer" in {
    // measurements: 0.575953, 0.60234, 0.574791, 0.618202, 0.626917, 0.612827, 0.612999, 0.638166, 0.574349, 0.620476, 0.573802, 0.626458, 0.620899, 0.612716, 0.624323, 0.576051, 0.574292, 0.595316, 0.596243, 0.620427, 0.596297, 0.581722, 0.575218, 0.594954, 0.574666, 0.574794, 0.597933, 0.581155, 0.621791, 0.595727, 0.575189, 0.600994, 0.573918, 0.576952, 0.593637, 0.623014
    measure method "create" in {
      using(dataGen) in { data =>
        Standardizer(features, data.iterator)
      }
    }

    // measurements: 0.10569, 0.107639, 0.105685, 0.105437, 0.105964, 0.107985, 0.078667, 0.076938, 0.077075, 0.077589, 0.078871, 0.076337, 0.07632, 0.076318, 0.076238, 0.076252, 0.076575, 0.081308, 0.076806, 0.076838, 0.076788, 0.07666, 0.077027, 0.077809, 0.081978, 0.076966, 0.076781, 0.077551, 0.076957, 0.077357, 0.077268, 0.077018, 0.077865, 0.077705, 0.078634, 0.10284
    measure method "merge" in {
      using(standardizersGen) in { standardizers =>
        standardizers.reduce(_ + _)
      }
    }
  }
}