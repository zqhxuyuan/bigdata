package com.chinahadoop.ml

/**
 * Created by chenchao on 14-3-24.
 */

import java.util.Random
import org.apache.spark.util.Vector
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.apache.spark.util.Vector

/**
 * K-means clustering.
 * 此例子会生成1000个向量，每个向量包含十个数字，任务是要选定十个向量作为质心
 */
object KMeans {
  val N = 1000
  val R = 1000    // Scaling factor
  val D = 10
  val K = 10
  val convergeDist = 0.001
  val rand = new Random(42)

  def generateData = {
    def generatePoint(i: Int) = {
      Vector(D, _ => rand.nextDouble * R)
    }
    Array.tabulate(N)(generatePoint)
  }

  def closestPoint(p: Vector, centers: HashMap[Int, Vector]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 1 to centers.size) {
      val vCurr = centers.get(i).get
      val tempDist = p.squaredDist(vCurr)
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  def main(args: Array[String]) {
    val data = generateData //初始化数据点，以向量形式存在，包含1000个向量，每个向量包含十个数字点
    var points = new HashSet[Vector] //随机选取的十组向量作为质心
    var kPoints = new HashMap[Int, Vector] //初始质心，共有十个 即每个质心对应着一个向量, 即这里的质心
    var tempDist = 1.0

    while (points.size < K) {
      points.add(data(rand.nextInt(N)))
    }

    val iter = points.iterator
    for (i <- 1 to points.size) {
      kPoints.put(i, iter.next())
    }

    println("Initial centers: " + kPoints)

    while(tempDist > convergeDist) {
      //查找与每个向量距离最近的质心
      var closest = data.map (p => (closestPoint(p, kPoints), (p, 1)))

      //把那些具有相同质心的向量聚合在一起
      var mappings = closest.groupBy[Int] (x => x._1)

      //把具有相同质心的向量标量相加
      var pointStats = mappings.map(pair => pair._2.reduceLeft [(Int, (Vector, Int))] {case ((id1, (x1, y1)), (id2, (x2, y2))) => (id1, (x1 + x2, y1+y2))})

      //找出十个新的质心
      var newPoints = pointStats.map {mapping => (mapping._1, mapping._2._1/mapping._2._2)}

      //迭代收敛条件，即质心变化不大
      tempDist = 0.0
      for (mapping <- newPoints) {
        tempDist += kPoints.get(mapping._1).get.squaredDist(mapping._2)
      }

      //将新质心放入kPoints
      for (newP <- newPoints) {
        kPoints.put(newP._1, newP._2)
      }
    }

    println("Final centers: " + kPoints)
  }
}
