package org.apache.spark.mllib.linalg

object VectorUtil {

  implicit class VectorPublications(val vector : org.apache.spark.mllib.linalg.Vector) extends AnyVal {
    def toBreeze : breeze.linalg.Vector[scala.Double] = vector.toBreeze

  }

  implicit class BreezeVectorPublications(val breezeVector : breeze.linalg.Vector[Double]) extends AnyVal {
    def toSpark : org.apache.spark.mllib.linalg.Vector = Vectors.fromBreeze(breezeVector)

    def toDenseSpark : org.apache.spark.mllib.linalg.Vector = breezeVector match {
      case v: breeze.linalg.DenseVector[Double] => v.toSpark
      case _ => breezeVector.toDenseVector.toSpark
    }

    def toDenseBreeze: breeze.linalg.DenseVector[Double] = breezeVector match {
      case v: breeze.linalg.DenseVector[Double] => v
      case _ => breezeVector.toDenseVector
    }
  }

  implicit class RichFloatArray(val arr: Array[Float]) extends AnyVal {
    def dot(other: Array[Float]): Float = {
      require(arr.length == other.length, "Trying to compute dot on arrays with different lengths")
      var sum = 0f
      val len = arr.length
      var i = 0
      while (i < len) {
        sum += arr(i) * other(i)
        i += 1
      }
      sum
    }

    def *(scalar: Float): Array[Float] = arr.map(f => f * scalar)

    def -(other: Array[Float]): Array[Float] = {
      require(arr.length == other.length, "Trying to compute dot on arrays with different lengths")
      (arr, other).zipped.map{ case (a, b) => a - b}
    }

    def +(other: Array[Float]): Array[Float] = {
      require(arr.length == other.length, "Trying to compute dot on arrays with different lengths")
      (arr, other).zipped.map{ case (a, b) => a + b}
    }
  }
}