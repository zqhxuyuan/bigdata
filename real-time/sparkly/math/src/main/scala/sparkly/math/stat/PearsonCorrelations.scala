package sparkly.math.stat

import breeze.linalg._
import breeze.numerics._

case class PearsonCorrelations (
  n: Double,
  sums: DenseVector[Double],
  squaredSums: DenseMatrix[Double]
) {

  def correlations(): DenseMatrix[Double] = {
    val c = sqrt((diag(squaredSums) * n) - (sums :* sums))
    ((squaredSums * n) - (sums * sums.t)) :/ (c * c.t)
  }

  def +(other: PearsonCorrelations): PearsonCorrelations = {
    val n = this.n + other.n
    val sums: DenseVector[Double] = this.sums + other.sums
    val squaredSums: DenseMatrix[Double] = this.squaredSums + other.squaredSums
    new PearsonCorrelations(n, sums, squaredSums)
  }

  def -(other: PearsonCorrelations): PearsonCorrelations = {
    val n = this.n - other.n
    val sums: DenseVector[Double] = this.sums - other.sums
    val squaredSums: DenseMatrix[Double] = this.squaredSums - other.squaredSums
    new PearsonCorrelations(n, sums, squaredSums)
  }
}

object PearsonCorrelations {

  def apply(data: DenseVector[Double]): PearsonCorrelations = {
    val n = 1
    val sums: DenseVector[Double] = data
    val squaredSums: DenseMatrix[Double] = (data * data.t)
    new PearsonCorrelations(n, sums, squaredSums)
  }

  def apply(data: Iterator[DenseVector[Double]]): PearsonCorrelations = {
    data.map(values => PearsonCorrelations(values)).reduce(_ + _)
  }

}
