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

package org.apache.spark.examples

import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}
import org.apache.spark.util.Vector

/**
  * Usage: BroadcastTest [slices] [numElem] [broadcastAlgo] [blockSize]
 *
 *  bin/run-example BroadcastTest
 *
 *  http://dongguo.me/blog/2014/12/30/Spark-Usage-Share/
 *  如果我们有一份const数据，需要在executors上用到，一个典型的例子是Driver从数据库中load了一份数据dbData，
 *  在很多RDD操作中都引用了dbData，这样的话，每次RDD操作，driver node都需要将dbData分发到各个executors node一遍，
 *  这非常的低效，特别是dbData比较大且RDD操作次数较多时。Spark的广播变量使得Driver可以提前只给各个executors node传一遍
  */
object BroadcastTest {
  def main(args: Array[String]) {

    val bcName = if (args.length > 2) args(2) else "Http"
    val blockSize = if (args.length > 3) args(3) else "4096"

    //参数slices表示将array分成几块,作为parallelize的第二个参数,表示任务的并行度
    //一个slice会由一个task运行
    val slices = if (args.length > 0) args(0).toInt else 2

    //构造一个原始的数组arr1
    val num = if (args.length > 1) args(1).toInt else 1000000
    val arr1 = (0 until num).toArray

    val sparkConf = new SparkConf().setAppName("Broadcast Test")
      //.setMaster("local[*]")
      .setMaster("spark://dp0652:7077")
      .set("spark.broadcast.factory", s"org.apache.spark.broadcast.${bcName}BroadcastFactory")
      .set("spark.broadcast.blockSize", blockSize)
    val sc = new SparkContext(sparkConf)

    //3次迭代,每次10个
    for (i <- 0 until 3) {
      println("Iteration " + i)
      println("===========")
      val startTime = System.nanoTime
      //声明一个广播变量barr1, 使用数组arr1传递进来. 在Driver程序里分发广播变量
      //参数必须是已经读取到driver程序里的变量，而不能是RDD
      val barr1 = sc.broadcast(arr1)

      //变量广播后应该使用广播后的名字barr1，而不是原变量名字arr1
      //广播变量可以让用户缓存一个只读变量(barr1)在每一台工作机器上，而不是将一个数据副本(arr1)传递给每一个task，能有效的为节点传递数据
      val observedSizes = sc.parallelize(1 to 10, slices)
        //在worker程序里，会通过barr1.value访问该数据副本
        //可以对barr1.value做各种操作,而不仅仅是求size这么简单的操作
        //barr1.value可以看做是缓存里的数据.
        .map(_ => barr1.value.size)

      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))

      barr1.unpersist()
    }
    sc.stop()
  }
}
