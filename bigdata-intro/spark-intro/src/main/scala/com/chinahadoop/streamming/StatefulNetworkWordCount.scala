package com.chinahadoop.streamming

import com.td.bigdata.spark.intro.SparkUtil
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

/**
 * Chen Chao
 */
object StatefulNetworkWordCount {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: StatefulNetworkWordCount <master> <hostname> <port> <seconds>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    //创建StreamingContext
    //val ssc = new StreamingContext(args(0), "StatefulNetworkWordCount", Seconds(args(3).toInt), System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))
    val ssc = SparkUtil.getStreamContext()
    ssc.checkpoint(".")

    //创建NetworkInputDStream，需要指定ip和端口
    val lines = ssc.socketTextStream(args(1), args(2).toInt)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))

    //使用updateStateByKey来更新状态
    val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
