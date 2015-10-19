package com.chinahadoop.spark

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext._

/**
 * Created by chenchao on 14-3-1.
 */
object Analysis{

  def main(args : Array[String]){

    if(args.length != 3){
      println("Usage : java -jar code.jar dependency_jars file_location save_location")
      System.exit(0)
    }

    val jars = ListBuffer[String]()
    args(0).split(',').map(jars += _)

    val conf = new SparkConf()
    conf.setMaster("spark://server1:8888")
        .setSparkHome("/data/software/spark-0.9.0-incubating-bin-hadoop1")
        .setAppName("analysis")
        .setJars(jars)
        .set("spark.executor.memory","25g")

    val sc = new SparkContext(conf)
    val data = sc.textFile(args(1))

    data.cache

    println(data.count)

    data.filter(_.split(' ').length == 3).map(_.split(' ')(1)).map((_,1)).reduceByKey(_+_)
    .map(x => (x._2, x._1)).sortByKey(false).map( x => (x._2, x._1)).saveAsTextFile(args(2))
  }

}
