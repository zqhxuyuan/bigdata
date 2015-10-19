/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.streaming

import java.util.HashMap

import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
 *      my-consumer-group topic1,topic2 1`
 */
object KafkaWordCount {
  def main(args: Array[String]) {
    //localhost:2181 my-consumer-group test 1
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    //DStream的时间窗口, 和后面的reduceByKeyAndWindow的时间窗户和滑动窗口没有任何关系!
    //比如DStream的时间窗口是1秒(跟下面的代码无关,只是为了举例), 而reduce window function的时间窗口=10秒,滑动窗口=2秒
    //从数据源过来的数据只能一秒钟产生一个RDD. 形成lines DStream
    //而window function窗口函数的计算跟数据源的时间窗口无关. 这个时间可以任意设定,根据业务规则调整.
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    //-------------------------------------
    //接收数据源Kafka的数据,构造DStream:lines. 和NetworkWordCount相比变化的只有这部分了.
    //即数据源不同,构造不同的input DStream. 最后在DStream上的处理方式都是一样的.

    //ZK集群, Kafka的消费者组标识, 消费主题列表, 线程
    val Array(zkQuorum, group, topics, numThreads) = args

    //每个主题用多少个分区进行消费.每个分区都在自己的线程中消费
    //TODO: 一个主题用一个分区来消费的含义是: ?? 一个分区消费一个主题? 只用一个线程来消费一个主题?
    //topic_name -> numPartitions
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //spark消费kafka的数据:spark的输入流来自于kafka集群.
    //从kafka中返回的DStream,我们只关心Tuple的第二个元素,里面是消息内容
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    //-------------------------------------
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      //每隔2秒统计过去10分钟的数据
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

// Produces some random words between 1 and 100.
object KafkaWordCountProducer {

  def main(args: Array[String]) {
    //localhost:9092 test 20 50
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " + "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }
    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    //序列化
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    while(true) {
      //每秒钟发送多少条消息
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        //每条消息的长度
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString).mkString(" ")
        println(str)
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
    }
  }

}
