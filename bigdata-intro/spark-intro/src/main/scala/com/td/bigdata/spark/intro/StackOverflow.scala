package com.td.bigdata.spark.intro

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
 * Created by zhengqh on 15/9/14.
 */
object StackOverflow {
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext._
  import sqlContext.implicits._

  //http://stackoverflow.com/questions/17621596/spark-whats-the-best-strategy-for-joining-a-2-tuple-key-rdd-with-single-key-rd
  def joinTwoRdd(): Unit ={
    val rdd1 = sc.parallelize(Seq((1, "A"), (2, "B"), (3, "C")))
    val rdd2 = sc.parallelize(Seq(((1, "Z"), 111), ((1, "ZZ"), 111), ((2, "Y"), 222), ((3, "X"), 333)))

    //1.map way
    val rdd1Broadcast = sc.broadcast(rdd1.collectAsMap())
    val joined = rdd2.mapPartitions({ iter =>
      val m = rdd1Broadcast.value
      for {
        ((t, w), u) <- iter
        if m.contains(t)
      } yield ((t, w), (u, m.get(t).get))
    }, preservesPartitioning = true)

    //2.rdd join way
    rdd1.join(rdd2.map {
      case ((t, w), u) => (t, (w, u))
    }).map {
      case (t, (v, (w, u))) => ((t, w), (u, v))
    }.collect()

    //3.zip way,like map
    val numSplits = 8
    val rdd_1 = sc.parallelize(Seq((1, "A"), (2, "B"), (3, "C"))).partitionBy(new HashPartitioner(numSplits))
    val rdd_2 = sc.parallelize(Seq(((1, "Z"), 111), ((1, "ZZ"), 111), ((1, "AA"), 123), ((2, "Y"), 222), ((3, "X"), 333))).partitionBy(new RDD2Partitioner(numSplits))

    val result = rdd_2.zipPartitions(rdd_1)(
      (iter2, iter1) => {
        val m = iter1.toMap
        for {
          ((t: Int, w), u) <- iter2
          if m.contains(t)
        } yield ((t, w), (u, m.get(t).get))
      }
    ).partitionBy(new HashPartitioner(numSplits))
    result.glom.collect
  }
  class RDD2Partitioner(partitions: Int) extends HashPartitioner(partitions) {
    override def getPartition(key: Any): Int = key match {
      case k: Tuple2[Int, String] => super.getPartition(k._1)
      case _ => super.getPartition(key)
    }
  }

  //http://stackoverflow.com/questions/25418715/trying-to-run-sparksql-over-spark-streaming
  case class Persons(name:String,age:Int)
  def sqlInStream(): Unit ={
    //sc.stop()
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.textFileStream("/user/qihuang.zheng/resources/stream")  //指定目录,不能是文件!
    //lines.foreachRDD(rdd=>rdd.foreach(println))

    // Create the FileInputDStream on the directory and use the stream to count words in new files created
    lines.foreachRDD(rdd=>{
      rdd.map(_.split(",")).map(p => Persons(p(0), p(1).trim.toInt)).toDF().registerTempTable("data")
      val teenagers = sql("SELECT name FROM data WHERE age >= 13 AND age <= 19")
      teenagers.collect().foreach(println)
    })

    ssc.start()
    ssc.stop(false)
    ssc.awaitTermination()
  }

  //http://stackoverflow.com/questions/25484879/sql-over-spark-streaming
  def streamSQL(): Unit ={
    //提前注册一张表
    val d1=sc.parallelize(Array(("a",10))).map(e=>Persons(e._1,e._2))
    d1.toDF().write.parquet("/user/qihuang.zheng/sql_stream")
    val parquet = sqlContext.read.parquet("/user/qihuang.zheng/sql_stream")
    parquet.toDF().registerTempTable("data")

    //流数据
    val ssc = new StreamingContext(sc, Seconds(2))
    val dStream = ssc.textFileStream("/user/qihuang.zheng/resources/stream")

    //写入到注册的表中, 最终写入到哪里?
    dStream.foreachRDD(rdd=>{
      rdd.toDF().write.saveAsTable("data")
    })

    sql("select * from data").show()
  }
}
