package com.td.bigdata.spark.clickcount

import java.util.Properties

import net.sf.json.JSONObject

import scala.collection.immutable.HashMap

//import org.codehaus.jettison.json.JSONObject

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

import scala.util.Random

/**
 * Kafka+Spark Streaming+Redis实时计算整合实践
 * http://shiyanjun.cn/archives/1097.html
 */
object KafkaEventProducer {

  private val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")

  private val random = new Random()

  private var pointer = -1

  def getUserID() : String = {
    pointer = pointer + 1
    if(pointer >= users.length) {
      pointer = 0
      users(pointer)
    } else {
      users(pointer)
    }
  }

  def click() : Double = {
    random.nextInt(10)
  }

  // bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic user_events --replication-factor 1 --partitions 1
  // bin/kafka-topics.sh --zookeeper localhost:2181 --list
  // bin/kafka-topics.sh --zookeeper localhost:2181 --describe user_events
  // bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic user_events --from-beginning
  def main(args: Array[String]): Unit = {
    val topic = "user_events"
    var brokers = "localhost:9092"

    //KafkaEventProducer 192.168.6.55:9092,192.168.6.56:9092,192.168.6.57:9092
    if (args.length > 0) {
      brokers = args(0)
    }

    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)

    while(true) {
      // prepare event data
      val event = new JSONObject()
//      event
//        .put("uid", getUserID)
//        .put("event_time", System.currentTimeMillis.toString)
//        .put("os_type", "Android")
//        .put("click_count", click)

      //TODO use net.sf.JSON
      val m = HashMap(
        "uid" -> getUserID(),
        "event_time" -> System.currentTimeMillis.toString,
        "os_type" -> "Android",
        "click_count" -> click
      )
      //event.putAll(m)

      //Just Json String 因为最终传递的是字符串,而不是json对象什么的,所以可以直接用字符串代替.
      val userId = getUserID()
      val eventT = System.currentTimeMillis.toString
      val clickV = click()
      val jsonStr =
        s"""
          |{
          |  "uid" : $userId,
          |  "event_time" : $eventT,
          |  "os_type" : "Android",
          |  "click_count" : $click
          |}
        """.stripMargin

      // produce event message
      //producer.send(new KeyedMessage[String, String](topic, event.toString))
      producer.send(new KeyedMessage[String, String](topic, jsonStr))
      println("Message sent: " + event)

      Thread.sleep(200)
    }
  }
}