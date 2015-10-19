package sparkly.math.stat

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Histogram {
  def apply(compression: Int, data: Iterable[Double]): Histogram = {
    data.foldLeft(new Histogram(compression))((hist, value) =>  hist.add(value))
  }
}

case class Histogram(compression: Int, centroids: List[Centroid] = List[Centroid](), min: Double = Double.MaxValue, max: Double = Double.MinValue) {

  def add(value: Double): Histogram = {
    val newCentroid = new Centroid(value, 1.0)
    val full: Boolean = this.compression == this.centroids.length

    val newCentroids = slideBy2(this.centroids){(head, e1, e2, tail) => (head, e1, e2, tail) match {
      case (_, Some(c1), _, _) if c1.p == value => Some(head += c1.incr() ++= tail)
      case (_, Some(c1), Some(c2), _) if !full && c1.p > value => Some(head += newCentroid += c1 += c2 ++= tail)
      case (_, Some(c1), None, _) if !full && c1.p > value => Some(head += newCentroid += c1)
      case (_, Some(c1), Some(c2), _) if full && (newCentroid.distance(c2) > newCentroid.distance(c1)) => Some(head += (c1 merge newCentroid) += c2 ++= tail)
      case (_, Some(c1), None, _) if full => Some(head += (c1 merge newCentroid))
      case (_, None, None, _) if !full => Some(head += newCentroid)
      case _ => None
    }}.get.toList

    this.copy(centroids = newCentroids, min = Math.min(value, this.min), max = Math.max(value, this.max))
  }

  def +(other: Histogram): Histogram = {
    var all = sortedConcat(this.centroids, other.centroids)

    while (all.length > compression) {
      var minDistanceIndex: Int = -1
      var minDistance: Double = Double.MaxValue

      var i: Int = 0
      while (i < all.length - 1) {
        val candidate: Double = all(i) distance all(i + 1)
        if (candidate < minDistance) {
          minDistanceIndex = i
          minDistance = candidate
        }
        i = i + 1
      }

      val c1 = minDistanceIndex
      val c2 = minDistanceIndex + 1

      val compressed = new Array[Centroid](all.length - 1)

      System.arraycopy(all, 0, compressed, 0, c1)
      compressed(c1) = all(c1) merge all(c2)
      System.arraycopy(all, c1 + 2, compressed, c1 + 1, all.length - c1 - 2)

      all = compressed
    }
    return this.copy(centroids = all.toList, min = Math.min(this.min, other.min), max = Math.max(this.max, other.max))
  }

  private def sortedConcat(a: List[Centroid], b: List[Centroid]): Array[Centroid] = {
    val aSize= a.size
    val bSize = b.size
    val result = new ArrayBuffer[Centroid](aSize + bSize)

    (0 until aSize + bSize).foldLeft((a, b, result)){(lists, k) => (lists._1, lists._2) match {
      case (Nil, Nil) => (Nil, Nil, lists._3)
      case (Nil, head :: tail) => lists._3.append(head); (Nil, tail, lists._3)
      case (head :: tail, Nil) => lists._3.append(head); (tail, Nil, lists._3)
      case (head1 :: tail1, head2 :: tail2) if head1.p < head2.p => lists._3.append(head1); (tail1, lists._2, lists._3)
      case (head1 :: tail1, head2 :: tail2) if head1.p >= head2.p => lists._3.append(head2); (tail2, lists._1, lists._3)
    }}._3.toArray
  }


  private def slideBy2[T, V](list: List[T])(op: (ListBuffer[T], Option[T], Option[T], List[T]) => Option[V]): Option[V] = {
    @tailrec def slideBy2Recur(head: ListBuffer[T], e1: Option[T], e2: Option[T], tail: List[T]): Option[V] = op(head, e1, e2, tail) match {
      case Some(list) => Some(list)
      case None if e1.isDefined => slideBy2Recur(head += e1.get, e2, if(tail.isEmpty) None else Some(tail.head), if(tail.isEmpty) Nil else tail.tail)
      case None => None
    }

    val (first, second, tail) = list match {
      case Nil => (None, None, Nil)
      case head :: tail if !tail.isEmpty => (Some(head), Some(tail.head), tail.tail)
      case head :: tail if tail.isEmpty => (Some(head), None, Nil)
    }
    slideBy2Recur(ListBuffer(), first, second, tail)
  }

  def bins(count: Int): List[Bin] = {
    val step = (max - min) / count

    (0 to count)
      .map(i => sum(Math.min(max, min + i * step)))
      .sliding(2).filter(_.size == 2)
      .map(pair => pair(1) - pair(0))
      .zipWithIndex
      .map{case (value, i) => Bin(min + i * step, min + (i + 1) * step, value)}
      .toList
  }

  private def sum(b: Double): Double = {
    if(b < centroids.head.p) return 0.0
    if(b >= centroids.last.p) return centroids.map(_.m).sum

    val ((c1, c2), i) = centroids
      .sliding(2).filter(_.size == 2)
      .map(pair => (pair(0), pair(1)))
      .zipWithIndex
      .find{case ((candidate1, candidate2), index) => (candidate1.p <= b && b <= candidate2.p)}
      .get

    val mb = c1.m + ((b - c1.p) * (c2.m - c1.m) / (c2.p - c1.p))
    var sum = ((c1.m + mb) / 2) * (b - c1.p) / (c2.p - c1.p)
    for (j <- 0 until i) {
      sum = sum + centroids(j).m
    }
    sum + c1.m / 2
  }

}

case class Bin(x1: Double, x2: Double, count: Double)

case class Centroid(p: Double, m: Double) {
  def merge(other: Centroid): Centroid = {
    val newCount = this.m + other.m
    val newPosition = (this.p * this.m + other.p * other.m) / newCount
    Centroid(newPosition, newCount)
  }

  def distance(other: Centroid): Double = Math.abs(this.p - other.p)

  def incr(): Centroid = this.copy(m = m + 1.0)
}

