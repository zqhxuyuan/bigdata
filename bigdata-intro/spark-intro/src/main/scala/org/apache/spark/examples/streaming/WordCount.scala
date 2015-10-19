package org.apache.spark.examples.streaming

import akka.actor.Props
import kafka.serializer.StringDecoder
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Time, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.SynchronizedQueue

/**
 * Created by zhengqh on 15/8/27.
 */
object WordCount {

  StreamingExamples.setStreamingLogLevels()
  val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  ssc.checkpoint("checkpoint")

  def main(args: Array[String]) {
    val mode = args(0)
    var lines : DStream[String] = null

    mode match {
      //NetworkWordCount
      case "NETWORK" => {
        val Array(_,host,port) = args
        lines = ssc.socketTextStream(host, port.toInt, StorageLevel.MEMORY_AND_DISK_SER)
      }

      //CustomReceiver
      case "NETWORK_CUSTOM" => {
        val Array(_,host,port) = args
        val lines = ssc.receiverStream(new CustomReceiver(host, port.toInt))
      }

      //ActorWordCount
      case "AKKA" => {
        val Array(_,host,port) = args
        lines = ssc.actorStream[String](
          Props(new SampleActorReceiver[String]("akka.tcp://test@%s:%s/user/FeederActor".format(host, port.toInt))), "SampleReceiver")
      }

      //KafkaWordCount
      case "KAFKA" => {
        val Array(zkQuorum, group, topics, numThreads) = args
        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

        lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
      }

      //DirectKafkaWordCount
      case "KAFKA_DIRECT" => {
        val Array(_,brokers, topics) = args
        val topicsSet = topics.split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

        lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, topicsSet).map(_._2)
      }

      //HdfsWordCount
      case "HDFS" => {
        val Array(_,directory) = args
        lines = ssc.textFileStream(directory)
      }

      //QueueStream
      case "QUEUE" => {
        val rddQueue = new SynchronizedQueue[RDD[String]]()
        lines = ssc.queueStream(rddQueue)
      }
    }

    wordcount(lines)

    ssc.start()

    //mock data here
    mockData(mode)

    ssc.awaitTermination()
  }

  def demoArgs(mode : String): Array[String] ={
    val network = Array[String]("NETWORK","localhost","9999")
    val kafka = Array[String]("KAFKA","localhost:2181","my-consumer-group","test","1")
    val hdfs = Array[String]("HDFS","/user/qihuang.zheng/")

    val args = mode match {
      case "NETWORK" => network
      case "KAFKA" => kafka
      case "HDFS" => hdfs
    }
    args
  }

  //对DStream进行计算, 包括窗口函数,保存状态,SQL查询
  def wordcount(lines : DStream[String]): Unit ={
    val wordCounts = lines.flatMap(_.split(" ")).map((word => (word, 1))).reduceByKey(_+_)
    wordCounts.print()
  }
  def wordcountWindow(lines : DStream[String]): Unit ={
    val wordCounts = lines.flatMap(_.split(" ")).map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(10), Seconds(2), 2)
    wordCounts.print()
  }
  def wordcountState(lines : DStream[String]): Unit ={
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    val stateDstream = wordDstream.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true, null)
    stateDstream.print()
  }
  def wordcountSQL(lines : DStream[String]): Unit ={
    val words = lines.flatMap(_.split(" "))
    words.foreachRDD((rdd: RDD[String], time: Time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val wordsDataFrame = rdd.map(w => Record(w)).toDF()
      wordsDataFrame.registerTempTable("words")
      val wordCountsDataFrame = sqlContext.sql("select word, count(*) as total from words group by word")
      wordCountsDataFrame.show()
    })
  }

  //模拟生成数据
  def mockData(mode : String): Unit ={

  }

  def dataToQueue(rddQueue : SynchronizedQueue[RDD[Int]]): Unit ={
    for (i <- 1 to 30) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10) //RDD[Int]
      Thread.sleep(100)
    }
  }
  def dataToKafka(): Unit ={
    KafkaWordCount.main(Array[String]("localhost:9092","test","20","50"))
  }

}
