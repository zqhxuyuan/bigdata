package com.td.bigdata.spark.call

/**
 * User: arnonrgo
 * Date: 12/7/14
 * Time: 10:55 AM
 *
 * https://github.com/Arnonrgo/spark-parquet-example
 * http://arnon.me/2014/09/apache-spark-parquet/
 */
trait Event {
  val year :Int
  val month : Int
  val day:Int
  val hour : Int
  val userId: Long

  def getHourly = f"$userId%012d-$year%04d-$month%02d-$day%02d-$hour%02d"
  def getDaily = f"$userId%012d-$year%04d-$month%02d-$day%02d"
}