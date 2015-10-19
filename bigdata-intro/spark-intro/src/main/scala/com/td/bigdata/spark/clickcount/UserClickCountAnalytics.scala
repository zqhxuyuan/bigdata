package com.td.bigdata.spark.clickcount

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UserClickCountAnalytics {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    var brokers = "localhost:9092"

    //UserClickCountAnalytics spark://dp0652:7077 192.168.6.55:9092,192.168.6.56:9092,192.168.6.57:9092
    if (args.length > 0) {
      masterUrl = args(0)
      brokers = args(1)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    //每隔5秒运行一次
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    val topics = Set("user_events")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder")

    // select 1
    // HGETALL app::users::click
    val dbIndex = 1
    val clickHashKey = "app::users::click"

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    //解析字符串为JSON对象. 根据输入,这里的json是Map结构
    val events = kafkaStream.flatMap(line => {
      val data = JSONObject.fromObject(line._2)
      Some(data)
    })

    // Compute user click times 在同一个窗口内计算相同用户的点击数
    val userClicks = events.map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_ + _)

    // 写入redis
    //userClicks是个DStream, 所以不能直接调用foreachPartition, 一个DStream由多个RDD组成. 每个RDD是一个时间窗口.
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          val uid = pair._1         //uid
          val clickCount = pair._2  //本次时间窗口的click_count
          val jedis = RedisClient.pool.getResource
          jedis.select(dbIndex)
          //在不同的时间窗口内, 相同的用户, 增加点击数. 这样最终数据库中的记录数不会增加, 只会更新记录.
          //key是不变的,只有一个值. field是user_id, value是clickCount.
          jedis.hincrBy(clickHashKey, uid, clickCount)

          //有状态的, 所有时间窗口
          val newCount = jedis.hget(clickHashKey, uid)
          println(newCount)
          RedisClient.pool.returnResource(jedis)
        })
      })
    })
    //userClicks.repartition(2).foreachPartition

    ssc.start()
    ssc.awaitTermination()
  }
}