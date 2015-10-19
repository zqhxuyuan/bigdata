package org.apache.spark.examples

import org.apache.spark.util.Vector
import org.apache.spark.{AccumulatorParam, SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by zhengqh on 15/8/18.
 */
object AccumulatorTest extends App{

  val sparkConf = new SparkConf().setAppName("Accumulator Test").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val result = mutable.MutableList[Int]()

  (0 to 5).foreach(init => {
    accumulator(init, Array(1, 2, 3, 4))
  })

  result.foreach(println)

  def accumulator(init : Int, array : Array[Int]): Unit ={
    //创建累加器, 第一个参数为初始值
    val accum = sc.accumulator(init, "My Accumulator")
    //任务在累加器上进行累加操作
    sc.parallelize(array).foreach(x => accum += x)
    //只有Driver可以读取累加器的值
    result += accum.value.toInt
  }

  def custorm(): Unit ={
    // Don't use scala.Vector(default import)
    object VectorAccumulatorParam extends AccumulatorParam[Vector] {
      def zero(initialValue: Vector): Vector = {
        Vector.zeros(initialValue.length)
      }
      def addInPlace(v1: Vector, v2: Vector): Vector = {
        v1 += v2
      }
    }

    // Then, create an Accumulator of this type:
    val vecAccum = sc.accumulator(new Vector(Array()))(VectorAccumulatorParam)
  }
}



