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

import scala.collection.mutable.SynchronizedQueue

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

object QueueStream {

  def main(args: Array[String]) {

    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local[2]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    //Create the queue through which RDDs can be pushed to a QueueInputDStream
    //Each RDD pushed into the queue will be treated as a batch of data in the DStream, and processed like a stream
    //放入队列里的每个RDD会被作为DStream中的一批数据. 一个RDD会作为一个Batch! 队列里的所有RDD被称作DStream
    val rddQueue = new SynchronizedQueue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)
    //inputStream的每条记录是RDD[Int]即1到1000
    val mappedStream = inputStream.map(x => (x % 10, 1))
    //以一个RDD为例,map的结果为1->(1,1),2->(2,1),...,11->(1,1),12->(2,1)...
    //reduceByKey的最后结果为:(1,100),(2,100),...
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()
    ssc.start()

    // Create and push some RDDs into
    for (i <- 1 to 30) {
      //每次创建一个RDD,并往队列中添加, 队列是一个DStream,但是添加的RDD才是计算的单元(batch)
      rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10) //RDD[Int]
      //这里的时间要和ssc的时间一样,用于控制产生RDD数据. TODO:如果把睡眠时间去掉或减少
      //会导致多个RDD同时往队列中写.这样在一个时间窗口内,会有多个RDD.
      //比如睡眠了0.1秒,则循环10次才1秒,这一秒的时间一共写入了10个RDD. 但是最终只有一个RDD参与计算!
      Thread.sleep(100)
    }

    //注意最终的大小并不一定等于30! 如果睡眠0.1秒,size=27. 如果睡眠1秒,size=0
    //队列大小如何确定: 当睡眠一秒时,每个RDD都在一秒的时间窗口内被处理了,即30次循环,队列中的30个RDD都被处理了.最后剩下0个RDD
    //当睡眠为0.1秒,队列中一共有30个RDD,但是循环一共耗时30*0.1=3秒,而Stream只处理3个RDD.最后剩下30-3=27个RDD
    //注意队列的一种性质: 消费队列中的一个元素,队列的大小减1.如果队列为空,则size=0
    println(rddQueue.size)

    ssc.stop()
  }
}
