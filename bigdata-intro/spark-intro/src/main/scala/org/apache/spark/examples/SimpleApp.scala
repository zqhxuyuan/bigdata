package org.apache.spark.examples

/**
 * Created by zhengqh on 15/7/9.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    //val logFile = "/Users/zhengqh/soft/spark-1.4.0/README.md" // Should be some file on your system
    //val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")

    //notice: 1. run in remote mode, you should't use local file. instead hdfs file
    val logFile = "hdfs://192.168.6.53:9000/user/qihuang.zheng/hello.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
        .setMaster("spark://dp0652:7077")
        //2.also the packaged jar file should include or else you will get NoClassDefined Exception
        //so before run this app, you should build current project. you can use IDE or maven/sbt command
        .setJars(Array("/Users/zhengqh/IdeaProjects/bigdata/out/artifacts/spark_intro/spark-intro.jar"))
    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}