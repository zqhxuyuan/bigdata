package sparkly.math.stat

import org.scalameter.api._
import org.scalatest._

import scala.util.Random

class HistogramSpec extends FlatSpec with Matchers {

  "Histogram" should "compute bins" in {
    // Given
    val data = (1 to 10000).map(index => 10 * Random.nextGaussian)
    val hist = Histogram(100, data)

    // When
    val bins = hist.bins(10)

    // Then
    val (expected, actual) = bins.map(bin => (expectedBin(data, bin.x1, bin.x2), bin.count)).unzip
    val errors = (expected, actual).zipped.map{case (e, a) => Math.pow(e - a, 2)}
    val rmse = Math.sqrt(errors.sum / errors.size) / expected.max

    hist.centroids should have size 100
    bins should have size 10
    rmse should be < 0.01
  }

  "Histogram" should "support reduce" in {
    // Given
    val data1 = (1 to 10000).map(index => 1 + 2 * Random.nextGaussian)
    val hist1 = Histogram(100, data1)

    val data2 = (1 to 10000).map(index => 2 + 50 * Random.nextGaussian)
    val hist2 = Histogram(100, data2)

    val data3 = (1 to 10000).map(index => 3 + 10 * Random.nextGaussian)
    val hist3 = Histogram(100, data3)

    val data = data1 ++ data2 ++ data3
    val hist = hist1 + hist2 + hist3

    // When
    val bins = hist.bins(10)

    // Then
    val (expected, actual) = bins.map(bin => (expectedBin(data, bin.x1, bin.x2), bin.count)).unzip
    val errors = (expected, actual).zipped.map{case (e, a) => Math.pow(e - a, 2)}
    val rmse = Math.sqrt(errors.sum / errors.size) / expected.max

    hist.centroids should have size 100
    bins should have size 10
    rmse should be < 0.1
  }

  def expectedBin(data: Iterable[Double], a: Double, b: Double): Double = {
    data.count(value => value > a && value < b).toDouble
  }
}

object HistogramBench extends PerformanceTest.Quickbenchmark {

  val size = 10000
  val compression = 100
  val partitions = 4

  val dataGen = Gen.single("data")((1 to size).map(i => Random.nextGaussian).toList)
  val histsGen = Gen.single("hists")(List.fill(partitions){
    val data = (1 to size).map(i => Random.nextGaussian)
    Histogram(compression, data)
  })
  val histGen = Gen.single("hist"){
    val data = (1 to size).map(i => Random.nextGaussian)
    Histogram(compression, data)
  }

  performance of "Running1DHistogram" in {
    // measurements: 0.438178, 0.462059, 0.463276, 0.44639, 0.501823, 0.430802, 0.430624, 0.479216, 0.433484, 0.424061, 0.43593, 0.423488, 0.423495, 0.439667, 0.441337, 0.481879, 0.438691, 0.425782, 0.428371, 0.42818, 0.436318, 0.426553, 0.455371, 0.430829, 0.42949, 0.432696, 0.451961, 0.46642, 0.503811, 0.496385, 0.611744, 0.680681, 0.693673, 0.70434, 0.671808, 0.706375
    measure method "add" in {
      using(dataGen) in { data =>
        val hist = new Histogram(compression)
        data.foreach(value => hist.add(value))
      }
    }

    // measurements: 0.23995, 0.243103, 0.2507, 0.24264, 0.275268, 0.343297, 0.239312, 0.239788, 0.250748, 0.237682, 0.238468, 0.238954, 0.247493, 0.254539, 0.239016, 0.236899, 0.27959, 0.244303, 0.243602, 0.243148, 0.237284, 0.239175, 0.241692, 0.237253, 0.277066, 0.242798, 0.235916, 0.23412, 0.236295, 0.235868, 0.236104, 0.233379, 0.233804, 0.238604, 0.237237, 0.267505
    measure method "merge" in {
      using(histsGen) in { histograms =>
        histograms.reduce(_ + _)
      }
    }

    // measurements: 0.606716, 0.591749, 0.588739, 0.691068, 0.642116, 0.663743, 0.644183, 0.63798, 1.421771, 0.692963, 0.702679, 0.71249, 0.708657, 0.707312, 0.694964, 0.722185, 0.696993, 0.793697, 0.818746, 0.813466, 0.840291, 0.81047, 0.777708, 0.815903, 1.032735, 1.060183, 1.073101, 1.065898, 1.19784, 1.808573, 1.771919, 1.72209, 1.757261, 1.486634, 1.334831, 1.493077
    measure method "bins" in {
      using(histGen) in { histogram =>
        histogram.bins(10)
      }
    }
  }
}
