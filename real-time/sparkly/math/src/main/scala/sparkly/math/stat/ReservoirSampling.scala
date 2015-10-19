package sparkly.math.stat

import scala.util.Random

object ReservoirSampling {

  def apply[T](capacity: Int): ReservoirSampling[T] = {
    ReservoirSampling[T](capacity, 0L)
  }

  def apply[T](capacity: Int, data: Iterable[T]): ReservoirSampling[T] = {
    ReservoirSampling[T](capacity, 0L).addAll(data)
  }
}
case class ReservoirSampling[T](capacity: Int, n: Long, samples: List[T] = List[T]()) {

  def add(e: T): ReservoirSampling[T] = {
    val count = n + 1
    val updated: List[T] = if(count <= capacity) {
      e :: samples
    } else {
      val index = nextLong(count)
      if(index < capacity) {
        val (head, tail) = samples.splitAt(index.toInt)
        e :: head ::: tail.tail
      } else {
        samples
      }
    }

    this.copy(n = count, samples = updated)
  }

  def addAll(data: Iterable[T]): ReservoirSampling[T] = {
    data.foldLeft(this)(_ add _)
  }

  def +(other: ReservoirSampling[T]): ReservoirSampling[T] = {
    val threshold = this.n / (this.n + other.n).toDouble
    val updated = (this.samples, other.samples).zipped.map{ case(v1, v2) => if(Random.nextDouble < threshold) v1 else v2}
    this.copy(n = this.n + other.n, samples = updated)
  }

  private def nextLong(n: Long): Long = {
    var bits: Long = 0L
    var value: Long = 0L
    do {
      bits = (Random.nextLong << 1) >>> 1
      value = bits % n
    } while (bits - value + (n-1) < 0L)
    value
  }
}
