package sparkly.math.classification

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class RunningAccuracy[T : ClassTag](successCount: Long = 0, total: Long = 0) {

  def update(actual: T, expected: T): RunningAccuracy[T] = {
    val success = if(expected == actual) 1 else 0
    RunningAccuracy[T](successCount + success, total + 1)
  }

  def update(actual: RDD[T], expected: RDD[T]): RunningAccuracy[T] = {
    this + expected.zip(actual)
      .map{case (e, a) =>  if(e == a) RunningAccuracy[T](1, 1) else RunningAccuracy[T](0, 1)}
      .reduce(_ + _)
  }

  def +(other: RunningAccuracy[T]): RunningAccuracy[T] = {
    RunningAccuracy[T](this.successCount + other.successCount, this.total + other.total)
  }

  def value = successCount.toDouble / total.toDouble
}

