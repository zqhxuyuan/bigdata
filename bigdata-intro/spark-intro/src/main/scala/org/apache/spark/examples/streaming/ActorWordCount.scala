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

import scala.collection.mutable.LinkedList
import scala.reflect.ClassTag
import scala.util.Random

import akka.actor.{Actor, ActorRef, Props, actorRef2Scala}

import org.apache.spark.{SparkConf, SecurityManager}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.util.AkkaUtils
import org.apache.spark.streaming.receiver.ActorHelper

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)

/**
 * Sends the random content to every receiver subscribed with 1/2 second delay.
 */
class FeederActor extends Actor {

  val rand = new Random()
  var receivers: LinkedList[ActorRef] = new LinkedList[ActorRef]()

  val strings: Array[String] = Array("words ", "may ", "count ")

  def makeMessage(): String = {
    val x = rand.nextInt(3)
    strings(x) + strings(2 - x)
  }

  /*
   * A thread to generate random messages 作为后台线程会模拟不断生产数据
   * 注意消息要发送的目标并不是当前的FeederActor.
   * 实际上是SampleActorReceiver. 即SampleActorReceiver会封装成SubscribeReceiver
   * 向remotePublisher=FeederActor发送消息. FeederActor处理消息时,将receiverActor加入到receivers中
   * 这样后台线程就可以模拟数据,往这些receivers即SampleActorReceiver发送消息了
   */
  new Thread() {
    override def run() {
      while (true) {
        Thread.sleep(500)
        receivers.foreach(_ ! makeMessage)
      }
    }
  }.start()

  //由于这是一个Actor,当向FeederActor发送消息的时候,该方法会被调用
  //什么时候会发消息给它: SampleActorReceiver的preStart和postStop方法.
  //因为SampleActorReceiver的remotePublisher是来自于ActorWordCount传递的参数:akka.tcp://test@%s:%s/user/FeederActor
  def receive: Receive = {

    case SubscribeReceiver(receiverActor: ActorRef) =>
      println("received subscribe from %s".format(receiverActor.toString))
    receivers = LinkedList(receiverActor) ++ receivers

    case UnsubscribeReceiver(receiverActor: ActorRef) =>
      println("received unsubscribe from %s".format(receiverActor.toString))
    receivers = receivers.dropWhile(x => x eq receiverActor)

  }
}

/**
 * A sample actor as receiver, is also simplest. This receiver actor
 * goes and subscribe to a typical publisher/feeder actor and receives data.
 *
 * @see [[org.apache.spark.examples.streaming.FeederActor]]
 */
class SampleActorReceiver[T: ClassTag](urlOfPublisher: String) extends Actor with ActorHelper {

  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)

  override def preStart(): Unit = remotePublisher ! SubscribeReceiver(context.self)

  //这里的接收消息来自于FeederActor的后台线程的模拟消息.
  def receive: PartialFunction[Any, Unit] = {
    case msg => store(msg.asInstanceOf[T])
  }

  override def postStop(): Unit = remotePublisher ! UnsubscribeReceiver(context.self)

}

/**
 * A sample feeder actor
 *
 * Usage: FeederActor <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder would start on.
 */
object FeederActor {

  def main(args: Array[String]) {
    if (args.length < 2){
      System.err.println("Usage: FeederActor <hostname> <port>\n")
      System.exit(1)
    }
    val Seq(host, port) = args.toSeq

    val conf = new SparkConf
    //创建一个ActorSystem, 这里的test名字会用于ActorWordCount的akka.tcp://test
    val actorSystem = AkkaUtils.createActorSystem("test", host, port.toInt, conf = conf,
      securityManager = new SecurityManager(conf))._1
    //创建一个Actor,模拟生产数据
    val feeder = actorSystem.actorOf(Props[FeederActor], "FeederActor")

    println("Feeder started as:" + feeder)

    actorSystem.awaitTermination()
  }
}

/**
 * A sample word count program demonstrating the use of plugging in
 * Actor as Receiver
 * Usage: ActorWordCount <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder is running on.
 *
 * To run this example locally, you may run Feeder Actor as
 *    `$ bin/run-example org.apache.spark.examples.streaming.FeederActor 127.0.1.1 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.ActorWordCount 127.0.1.1 9999`
 */
object ActorWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ActorWordCount <hostname> <port>")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()

    val Seq(host, port) = args.toSeq
    val sparkConf = new SparkConf().setAppName("ActorWordCount")
    // Create the context and set the batch size
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    /*
     * Following is the use of actorStream to plug in custom actor as receiver
     *
     * An important point to note:
     * Since Actor may exist outside the spark framework, It is thus user's responsibility
     * to ensure the type safety, i.e type of data received and InputDstream should be same.
     *
     * For example: Both actorStream and SampleActorReceiver are parameterized
     * to same type to ensure type safety.
     */

    //数据源来自于Actor. 参数是一个Actor的URL.
    val lines = ssc.actorStream[String](
      Props(new SampleActorReceiver[String]("akka.tcp://test@%s:%s/user/FeederActor".format(
        host, port.toInt))), "SampleReceiver")

    // compute wordcount
    lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
