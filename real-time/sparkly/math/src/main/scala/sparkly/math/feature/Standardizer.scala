package sparkly.math.feature

import breeze.linalg._
import breeze.numerics._

object Standardizer {
  def apply(size: Int): Standardizer = {
    Standardizer(size, 0, DenseVector.zeros[Double](size), DenseVector.zeros[Double](size))
  }

  def apply(size: Int, data: Iterator[Vector[Double]]): Standardizer = {
    var standardizer = Standardizer(size, 0, DenseVector.zeros[Double](size), DenseVector.zeros[Double](size))
    data.foreach { vector =>
      standardizer = standardizer.add(vector)
    }
    standardizer
  }
}

case class Standardizer(size: Int, n: Long, mu: Vector[Double], mu2: Vector[Double]) {

  def standardize(value: Vector[Double]): Vector[Double] = (value - mu) / stddev
  def inverse(standardized: Vector[Double]): Vector[Double] = (standardized :* stddev) + mu

  lazy val stddev: Vector[Double] = {
    if (n == 0) {
      DenseVector.fill(size, Double.PositiveInfinity)
    } else {
      sqrt(mu2.toDenseVector :/ n.toDouble).map(v => if(v == 0.0) Double.PositiveInfinity else v)
    }
  }

  def add(values: Vector[Double]): Standardizer = {
    val newN = this.n + 1
    val newDelta = values - this.mu
    val newMu = this.mu + (newDelta :/ newN.toDouble)
    val newMu2 = this.mu2 + (newDelta :* (values - newMu))
    this.copy(n = newN, mu = newMu, mu2 = newMu2)
  }

  def +(other: Standardizer): Standardizer = {
    if (this.n == 0) {
      other
    } else if (other.n == 0) {
      this
    } else {
      val delta = other.mu - this.mu
      val mu = if (other.n * 10 < this.n) {
        this.mu + ((delta * other.n.toDouble) / (this.n + other.n).toDouble)
      } else if (this.n * 10 < other.n) {
        other.mu - (delta * this.n.toDouble) / (this.n.toDouble + other.n.toDouble)
      } else {
        ((this.mu * this.n.toDouble) + (other.mu * other.n.toDouble)) :/ (this.n + other.n).toDouble
      }
      val mu2 = this.mu2 + other.mu2 + (((delta :* delta) * (this.n * other.n).toDouble) / (this.n + other.n).toDouble)
      val n = this.n + other.n
      Standardizer(this.size, n, mu, mu2)
    }
  }

}