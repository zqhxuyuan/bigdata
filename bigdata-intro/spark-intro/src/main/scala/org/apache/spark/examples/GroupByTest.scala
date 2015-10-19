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

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
  * Usage: GroupByTest [numMappers] [numKVPairs] [KeySize] [numReducers]
  */
object GroupByTest {
  def main(args: Array[String]) {
    //1.run in local mode
    //val sparkConf = new SparkConf().setAppName("GroupBy Test").setMaster("local[*]")
    //2.run in remote mode
    val sparkConf = new SparkConf().setAppName("GroupBy Test").setMaster("spark://dp0652:7077")
      .setJars(Array("/Users/zhengqh/IdeaProjects/bigdata/out/artifacts/spark_intro/spark-intro.jar"))
    var numMappers = if (args.length > 0) args(0).toInt else 2
    var numKVPairs = if (args.length > 1) args(1).toInt else 1000
    var valSize = if (args.length > 2) args(2).toInt else 1000
    var numReducers = if (args.length > 3) args(3).toInt else numMappers

    val sc = new SparkContext(sparkConf)

    //默认有2个mapper,利用parallelize,相当于又是一个集合的循环.
    //外层循环有2次(numMappers), 内层的数组个数(numKVPairs)有1000个,所以总的个数有2*1000=2000个
    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      //数组里的元素类型是一个KV pair,数组个数默认是1000个
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      //上面只是初始化数组,下面要对数组的每个元素填充数据,所以循环的次数和数组个数是一样的
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        //填充数据,key是一个随机数,value是一个字节数组Array[Byte]
        arr1(i) = (ranGen.nextInt(5), byteArr)
      }
      //最后的返回值就是这个数组
      arr1
    }.cache()

    /**
    (4,[B@54194b0e)
    (2,[B@488eaf44)
    (3,[B@6e6bace2)
    (2,[B@bfb393c)
    (1,[B@1a6a9849)
    (1,[B@65597022)
    (0,[B@52d0fc29)
    (2,[B@165b1239)
    (1,[B@350967a6)
    (1,[B@7c73de7d)
    */
    pairs1.take(10).foreach(println)
    // Enforce that everything has been calculated and in cache
    //为什么要在groupByKey之前调用rdd.count?
    //因为pairs1是经过cache后的rdd, 只有第一次执行Action操作后才会缓存. 
    //如果没有执行pairs1.count, 则下面的groupByKey第一次执行会慢点,第二次之后才会使用cache里的rdd
    println("total count:"+pairs1.count())  //always 2000
    
    //groupByKey的参数表示partition的个数,因为pairs1=[randomKey, byteArr]
    //而randomKey很少会重复(如果nextInt的参数小的话才可能重复),所以count结果一般等于pairs1的个数=2000
    //如果key发生了重复,这样group by key, count对于相同的key的多条记录,只统计一次,则count结果会小于2000

    //LOGGER: INFO DAGScheduler: Got job 1 (count at GroupByTest.scala:56) with 2 output partitions (allowLocal=false)
    println("group count:"+pairs1.groupByKey(numReducers).count())

    sc.stop()
  }
}
