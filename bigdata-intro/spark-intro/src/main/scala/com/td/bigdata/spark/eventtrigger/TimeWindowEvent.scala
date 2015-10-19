package com.td.bigdata.spark.eventtrigger

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * Created by zqhxuyuan on 15-9-17.
 * https://damieng.com/blog/2015/06/27/time-window-events-with-apache-spark-streaming
 */
object TimeWindowEvent {

  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    var brokers = "localhost:9092"

    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val scc = new StreamingContext(conf, Minutes(1))

    val topics = Set("user_events")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParams, topics)

    var messageLimit : Int = 10
    var messageWindow = Minutes(5)

    // ... setup Kafka consumer via SparkUtils
    kafkaStream.flatMap(line=>{
      val data = Msg("DDOS","1.1.1.1","1.1.1.2","1223344555")
      Some(data)
    }).filter(m => m.securityType == "DDOS")
      .map(m => m.targetIp -> Seq((m.timestamp, m.sourceIp)))
      .reduceByKeyAndWindow({(x, y) => x ++ y}, messageWindow)
      .filter(g => g._2.length >= messageLimit)
      .foreachRDD(m => m.foreach(println))

    kafkaStream.flatMap(line=>{
      val data = Msg("DDOS","1.1.1.1","1.1.1.2","1223344555")
      Some(data)
    })
      .filter(m => m.securityType == "DDOS")
      .map(m => m.targetIp -> WindowEventTrigger(Seq(m.timestamp, m.sourceIp),true,Some("TEST"), messageLimit))
      .reduceByKeyAndWindow(_ add _, _ remove _, messageWindow)
      .filter(_._2.triggerNow)
      .foreachRDD(m => m.foreach(println))
    scc.start()
  }

}

case class Msg(securityType:String,targetIp:String,sourceIp:String,timestamp:String)

case class WindowEventTrigger[T] (
                                          eventsInWindow: Seq[T],
                                          triggerNow: Boolean=true,
                                          private val lastTriggeredEvent: Option[T],
                                          private val triggerLevel: Int) {
  def this(item: T, triggerLevel: Int) = this(Seq(item), false, None, triggerLevel)

  def add(incoming: WindowEventTrigger[T]): WindowEventTrigger[T] = {
    val combined = eventsInWindow ++ incoming.eventsInWindow
    val shouldTrigger = lastTriggeredEvent.isEmpty && combined.length >= triggerLevel
    val triggeredEvent = if (shouldTrigger) combined.seq.drop(triggerLevel - 1).headOption else lastTriggeredEvent
    new WindowEventTrigger(combined, shouldTrigger, triggeredEvent, triggerLevel)
  }

  def remove(outgoing: WindowEventTrigger[T]): WindowEventTrigger[T] = {
    val reduced = eventsInWindow.filterNot(y => outgoing.eventsInWindow.contains(y))
    val triggeredEvent = if (lastTriggeredEvent.isDefined && outgoing.eventsInWindow.contains(lastTriggeredEvent.get)) None else lastTriggeredEvent
    new WindowEventTrigger(reduced, false, triggeredEvent, triggerLevel)
  }
}
