package com.td.bigdata.spark.intro

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.MutableList

/**
 * Created by zhengqh on 15/7/24.
 */
object SparkBasicTest {

  //-----------------------------------------
  //Spark RDD UNION Demo
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext._
  import sqlContext.implicits._

  def testRddUnion(): Unit = {
    //模拟相同sequenceId,不同指标放在Map中. 合并
    val a1 = (1, Map("p1"->1, "p2"->2))
    val a2 = (1, Map("p3"->3))
    val l1 = List(a1,a2)

    val a11 = (1, Map("p4"->4))
    val a12 = (1, Map("p5"->5,"p6"->6))
    val l2 = List(a11,a12)

    val rdd1 = sc.parallelize(l1) //RDD[(Int, scala.collection.immutable.Map[String,Int])] = ParallelCollectionRDD[4]
    val rdd2 = sc.parallelize(l2)

    //我们的目标是: (1, Map("p1"->1, "p2"->2), Map("p3"->3), Map("p4"->4), Map("p5"->5,"p6"->6))
    //或者更进一步: (1, Map("p1"->1, "p2"->2, "p3"->3, "p4"->4, "p5"->5,"p6"->6))
    //将不同的rdd合并用union, 最后相同id的分在同一组, 用group by key

    rdd1.collect() //Array[(Int, scala.collection.immutable.Map[String,Int])] = Array((1,Map(p1 -> 1, p2 -> 2)), (1,Map(p3 -> 3)))
    rdd2.collect() //Array[(Int, scala.collection.immutable.Map[String,Int])] = Array((1,Map(p4 -> 4)), (1,Map(p5 -> 5, p6 -> 6)))

    val rdd3 = rdd1.union(rdd2) //RDD[(Int, scala.collection.immutable.Map[String,Int])] = UnionRDD[7]
    rdd3.collect() // Array[(Int, scala.collection.immutable.Map[String,Int])] = Array((1,Map(p1 -> 1, p2 -> 2)), (1,Map(p3 -> 3)), (1,Map(p4 -> 4)), (1,Map(p5 -> 5, p6 -> 6)))


    val a21 = (2, Map("p1" -> 1, "p2" -> 2))
    val a22 = (2, Map("p3" -> 3))
    val l3 = List(a11, a12, a21, a22)
    val rdd20 = sc.parallelize(l3)
    val rdd4 = rdd1.union(rdd20)
    rdd4.collect() //Array((1,Map(p1 -> 1, p2 -> 2)), (1,Map(p3 -> 3)), (1,Map(p4 -> 4)), (1,Map(p5 -> 5, p6 -> 6)), (2,Map(p1 -> 1, p2 -> 2)), (2,Map(p3 -> 3)))

    //union操作并不会改变数据放入时的状态. 而且相同key不会有聚合的行为. 为了使得相同key在同一行. 要使用xxxByKey函数
    //groupByKey只是简单地聚合原来的数据
    val rdd41 = rdd4.groupByKey()
    rdd41.collect() //Array[(Int, Iterable[scala.collection.immutable.Map[String,Int]])] = Array((1,CompactBuffer(Map(p5 -> 5, p6 -> 6), Map(p4 -> 4), Map(p3 -> 3), Map(p1 -> 1, p2 -> 2))), (2,CompactBuffer(Map(p1 -> 1, p2 -> 2), Map(p3 -> 3))))

    //reduceByKey可以自定义类型. byKey的话,key自动的就是Tuple中的第一个元素
    val rdd42 = rdd4.reduceByKey((m1, m2) => {
      //如果有key相同的情况呢? 比如求和. 则以一个map为基础,根据key,去另外一个map里找有没有对应的元素. 然后将两个map相同key的value相加
      m1 ++ m2.map { case (k, v) => k -> (v + m1.getOrElse(k, 0)) }
      m1 ++ m2.map { case (k, v) => k -> Math.max(v, m1.getOrElse(k, 0)) }

      m1 ++ m2 //两个Map合并为一个Map.
    }) //Array[(Int, scala.collection.immutable.Map[String,Int])] = Array((1,Map(p4 -> 4, p5 -> 5, p3 -> 3, p2 -> 2, p1 -> 1, p6 -> 6)), (2,Map(p1 -> 1, p2 -> 2, p3 -> 3)))


    val b01 = (1, Map("p10"->10, "p11"->11))
    val b02 = (1, Map("p12"->12, "p13"->13))
    val l01 = sc.parallelize(List(b01,b02)) //RDD[(Int,Map)]

    val b03 = (1, Map("p20"->20, "p21"->11))
    val b04 = (1, Map("p22"->22, "p23"->13))
    val b05 = (2, Map("p20"->20, "p21"->11))
    val b06 = (2, Map("p22"->22, "p23"->13))

    val l02 = sc.parallelize(List(b03,b04,b05,b06))
    val rdd5 = l01.union(l02) //RDD[(Int,Map)]

    //RDD[(Int,Map)]的reduce方法,参数必须是RDD里的每个元素. 由于是reduce,所以两个参数都是(Int,Map)
    rdd5.reduce(redFun)
    def redFun(a: (Int,Map[String,Int]), b: (Int,Map[String,Int])): (Int,Map[String,Int]) ={
      a
    }
  }

  def unionAndGroupTest(): Unit ={
    import org.apache.spark.HashPartitioner
    sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val beta = 0.8
    val numOfPartition = 2
    val data = Seq(("A", "B"), ("A", "C"), ("B", "C"), ("C", "A"))

    val links = sc.parallelize(data).groupByKey.partitionBy(new HashPartitioner(numOfPartition)).persist
    var ranks = links.mapValues(_ => 1.0)

    var leakedMatrix = links.mapValues(_ => (1.0-beta)).persist
    leakedMatrix.partitioner
    links.partitioner

    for (i <- 1 until 2) {
      val contributions = links.join(ranks).flatMap {
        case (pageId, (links, rank)) =>
          links.map(dest => (dest, rank / links.size * beta))
      }
      ranks = contributions.union(leakedMatrix).reduceByKey(_ + _)
    }
    ranks.count
  }

  //-------------------------------------
  //模拟生成RDD
  def makeRdd(dim : String, gap : Int): RDD[(Int,Map[String,Int])] ={
    val pre = dim + gap  //代表不同维度,不同时间窗口/次数窗口的回溯
    val b01 = (1, Map(pre+"p10"->10, pre+"p11"->11))  //p10,p11..都是一些计算指标
    val b02 = (1, Map(pre+"p12"->12, pre+"p13"->13))
    val l01 = sc.parallelize(List(b01,b02))

    //val b03 = (1, Map(pre+"p20"->20, pre+"p21"->11))  //在不同的RDD中,相同seqId,有不同的指标
    //val b04 = (1, Map(pre+"p22"->22, pre+"p23"->13))
    val b05 = (2, Map(pre+"p20"->20, pre+"p21"->11))
    val b06 = (2, Map(pre+"p22"->22, pre+"p23"->13))

    val l02 = sc.parallelize(List(b05,b06))
    val rdd4 = l01.union(l02)
    rdd4
  }

  //java.lang.OutOfMemoryError: GC overhead limit exceeded
  //TODO BIGDATA
  def makeBigRdd(dim : String, gap : Int): RDD[(Int,Map[String,Int])] ={
    val pre = dim + gap
    val big = Range(1,35000000)
    val met = Range(1,25)
    val list = MutableList[(Int,Map[String,Int])]()
    //3000万*25=75000万=7.5亿!!!
    big.map(id=>{
      met.map(m=>{
        list += ((id, Map(pre+"_met"+met -> id)))
      })
    })
    val rdd1 = sc.parallelize(list)
    rdd1
  }

  //---------------------------------------
  //groupByKey and reduceByKey
  def demoKoudaiMetric: Unit = {
    val freqList = List(2, 5, 10)
    val timeList = List(60, 180, 360, 3600, 59200)
    val dimensionList = List("account", "deviceId", "mobile")
    val freqPairs = for (freq <- freqList; dim <- dimensionList) yield (dim, freq)
    val timePairs = for (time <- timeList; dim <- dimensionList) yield (dim, time)

    val freqRddList = freqPairs.map(p => makeRdd(p._1, p._2))
    val unionRdd1 = freqRddList.reduce((a, b) => a.union(b))

    val timeRddList = timePairs.map(p => makeRdd(p._1, p._2))
    val unionRdd2 = timeRddList.reduce((a, b) => a.union(b))

    val unionRdd = unionRdd1.union(unionRdd2) //RDD[Int,Map[String,Int]]

    //**********groupByKey**********
    val mergeRdd = unionRdd.groupByKey() //RDD[(Int, Iterable[Map[String,Int]])] = ShuffledRDD[199]

    /**
     * Iterable中的结构还保留了数据添加时的结果.能不能扁平化? 变成只有一个Map. 或者针对每个维度一个map
    (1,CompactBuffer(Map(account2p10 -> 10, account2p11 -> 11), Map(account2p12 -> 12, account2p13 -> 13), Map(account2p20 -> 20, account2p21 -> 11), Map(account2p22 -> 22, account2p23 -> 13), Map(deviceId2p10 -> 10, deviceId2p11 -> 11), Map(deviceId2p12 -> 12, deviceId2p13 -> 13), Map(deviceId2p20 -> 20, deviceId2p21 -> 11), Map(deviceId2p22 -> 22, deviceId2p23 -> 13), Map(mobile2p10 -> 10, mobile2p11 -> 11), Map(mobile2p12 -> 12, mobile2p13 -> 13), Map(mobile2p20 -> 20, mobile2p21 -> 11), Map(mobile2p22 -> 22, mobile2p23 -> 13), Map(account5p10 -> 10, account5p11 -> 11), Map(account5p12 -> 12, account5p13 -> 13), Map(account5p20 -> 20, account5p21 -> 11), Map(account5p22 -> 22, account5p23 -> 13), Map(deviceId5p10 -> 10, deviceId5p11 -> 11), Map(deviceId5p12 -> 12, deviceId5p13 -> 13), Map(deviceId5p20 -> 20, deviceId5p21 -> 11), Map(deviceId5p22 -> 22, deviceId5p23 -> 13), Map(mobile5p10 -> 10, mobile5p11 -> 11), Map(mobile5p12 -> 12, mobile5p13 -> 13), Map(mobile5p20 -> 20, mobile5p21 -> 11), Map(mobile5p22 -> 22, mobile5p23 -> 13), Map(account10p10 -> 10, account10p11 -> 11), Map(account10p12 -> 12, account10p13 -> 13), Map(account10p20 -> 20, account10p21 -> 11), Map(account10p22 -> 22, account10p23 -> 13), Map(deviceId10p10 -> 10, deviceId10p11 -> 11), Map(deviceId10p12 -> 12, deviceId10p13 -> 13), Map(deviceId10p20 -> 20, deviceId10p21 -> 11), Map(deviceId10p22 -> 22, deviceId10p23 -> 13), Map(mobile10p10 -> 10, mobile10p11 -> 11), Map(mobile10p12 -> 12, mobile10p13 -> 13), Map(mobile10p20 -> 20, mobile10p21 -> 11), Map(mobile10p22 -> 22, mobile10p23 -> 13), Map(account60p10 -> 10, account60p11 -> 11), Map(account60p12 -> 12, account60p13 -> 13), Map(account60p20 -> 20, account60p21 -> 11), Map(account60p22 -> 22, account60p23 -> 13), Map(deviceId60p10 -> 10, deviceId60p11 -> 11), Map(deviceId60p12 -> 12, deviceId60p13 -> 13), Map(deviceId60p20 -> 20, deviceId60p21 -> 11), Map(deviceId60p22 -> 22, deviceId60p23 -> 13), Map(mobile60p10 -> 10, mobile60p11 -> 11), Map(mobile60p12 -> 12, mobile60p13 -> 13), Map(mobile60p20 -> 20, mobile60p21 -> 11), Map(mobile60p22 -> 22, mobile60p23 -> 13), Map(account180p10 -> 10, account180p11 -> 11), Map(account180p12 -> 12, account180p13 -> 13), Map(account180p20 -> 20, account180p21 -> 11), Map(account180p22 -> 22, account180p23 -> 13), Map(deviceId180p10 -> 10, deviceId180p11 -> 11), Map(deviceId180p12 -> 12, deviceId180p13 -> 13), Map(deviceId180p20 -> 20, deviceId180p21 -> 11), Map(deviceId180p22 -> 22, deviceId180p23 -> 13), Map(mobile180p10 -> 10, mobile180p11 -> 11), Map(mobile180p12 -> 12, mobile180p13 -> 13), Map(mobile180p20 -> 20, mobile180p21 -> 11), Map(mobile180p22 -> 22, mobile180p23 -> 13), Map(account360p10 -> 10, account360p11 -> 11), Map(account360p12 -> 12, account360p13 -> 13), Map(account360p20 -> 20, account360p21 -> 11), Map(account360p22 -> 22, account360p23 -> 13), Map(deviceId360p10 -> 10, deviceId360p11 -> 11), Map(deviceId360p12 -> 12, deviceId360p13 -> 13), Map(deviceId360p20 -> 20, deviceId360p21 -> 11), Map(deviceId360p22 -> 22, deviceId360p23 -> 13), Map(mobile360p10 -> 10, mobile360p11 -> 11), Map(mobile360p12 -> 12, mobile360p13 -> 13), Map(mobile360p20 -> 20, mobile360p21 -> 11), Map(mobile360p22 -> 22, mobile360p23 -> 13), Map(account3600p10 -> 10, account3600p11 -> 11), Map(account3600p12 -> 12, account3600p13 -> 13), Map(account3600p20 -> 20, account3600p21 -> 11), Map(account3600p22 -> 22, account3600p23 -> 13), Map(deviceId3600p10 -> 10, deviceId3600p11 -> 11), Map(deviceId3600p12 -> 12, deviceId3600p13 -> 13), Map(deviceId3600p20 -> 20, deviceId3600p21 -> 11), Map(deviceId3600p22 -> 22, deviceId3600p23 -> 13), Map(mobile3600p10 -> 10, mobile3600p11 -> 11), Map(mobile3600p12 -> 12, mobile3600p13 -> 13), Map(mobile3600p20 -> 20, mobile3600p21 -> 11), Map(mobile3600p22 -> 22, mobile3600p23 -> 13), Map(account59200p10 -> 10, account59200p11 -> 11), Map(account59200p12 -> 12, account59200p13 -> 13), Map(account59200p20 -> 20, account59200p21 -> 11), Map(account59200p22 -> 22, account59200p23 -> 13), Map(deviceId59200p10 -> 10, deviceId59200p11 -> 11), Map(deviceId59200p12 -> 12, deviceId59200p13 -> 13), Map(deviceId59200p20 -> 20, deviceId59200p21 -> 11), Map(deviceId59200p22 -> 22, deviceId59200p23 -> 13), Map(mobile59200p10 -> 10, mobile59200p11 -> 11), Map(mobile59200p12 -> 12, mobile59200p13 -> 13), Map(mobile59200p20 -> 20, mobile59200p21 -> 11), Map(mobile59200p22 -> 22, mobile59200p23 -> 13)))
    (2,CompactBuffer(Map(account2p20 -> 20, account2p21 -> 11), Map(account2p22 -> 22, account2p23 -> 13), Map(deviceId2p20 -> 20, deviceId2p21 -> 11), Map(deviceId2p22 -> 22, deviceId2p23 -> 13), Map(mobile2p20 -> 20, mobile2p21 -> 11), Map(mobile2p22 -> 22, mobile2p23 -> 13), Map(account5p20 -> 20, account5p21 -> 11), Map(account5p22 -> 22, account5p23 -> 13), Map(deviceId5p20 -> 20, deviceId5p21 -> 11), Map(deviceId5p22 -> 22, deviceId5p23 -> 13), Map(mobile5p20 -> 20, mobile5p21 -> 11), Map(mobile5p22 -> 22, mobile5p23 -> 13), Map(account10p20 -> 20, account10p21 -> 11), Map(account10p22 -> 22, account10p23 -> 13), Map(deviceId10p20 -> 20, deviceId10p21 -> 11), Map(deviceId10p22 -> 22, deviceId10p23 -> 13), Map(mobile10p20 -> 20, mobile10p21 -> 11), Map(mobile10p22 -> 22, mobile10p23 -> 13), Map(account60p20 -> 20, account60p21 -> 11), Map(account60p22 -> 22, account60p23 -> 13), Map(deviceId60p20 -> 20, deviceId60p21 -> 11), Map(deviceId60p22 -> 22, deviceId60p23 -> 13), Map(mobile60p20 -> 20, mobile60p21 -> 11), Map(mobile60p22 -> 22, mobile60p23 -> 13), Map(account180p20 -> 20, account180p21 -> 11), Map(account180p22 -> 22, account180p23 -> 13), Map(deviceId180p20 -> 20, deviceId180p21 -> 11), Map(deviceId180p22 -> 22, deviceId180p23 -> 13), Map(mobile180p20 -> 20, mobile180p21 -> 11), Map(mobile180p22 -> 22, mobile180p23 -> 13), Map(account360p20 -> 20, account360p21 -> 11), Map(account360p22 -> 22, account360p23 -> 13), Map(deviceId360p20 -> 20, deviceId360p21 -> 11), Map(deviceId360p22 -> 22, deviceId360p23 -> 13), Map(mobile360p20 -> 20, mobile360p21 -> 11), Map(mobile360p22 -> 22, mobile360p23 -> 13), Map(account3600p20 -> 20, account3600p21 -> 11), Map(account3600p22 -> 22, account3600p23 -> 13), Map(deviceId3600p20 -> 20, deviceId3600p21 -> 11), Map(deviceId3600p22 -> 22, deviceId3600p23 -> 13), Map(mobile3600p20 -> 20, mobile3600p21 -> 11), Map(mobile3600p22 -> 22, mobile3600p23 -> 13), Map(account59200p20 -> 20, account59200p21 -> 11), Map(account59200p22 -> 22, account59200p23 -> 13), Map(deviceId59200p20 -> 20, deviceId59200p21 -> 11), Map(deviceId59200p22 -> 22, deviceId59200p23 -> 13), Map(mobile59200p20 -> 20, mobile59200p21 -> 11), Map(mobile59200p22 -> 22, mobile59200p23 -> 13)))
     */
    mergeRdd.collect //Array[(Int, Iterable[Map[String,Int]])]
    mergeRdd.collect.foreach(println)
    mergeRdd.count //2

    //因为mergeRdd还是一个RDD[(Int,Iterable)]的元组,所以看看能不能把Iterable里的Map扁平化下
    /**
    flatten扁平化: RDD[(Int, List[(String, Int)])] = MapPartitionsRDD[203]
    (1,List((account2p10,10), (account2p11,11), (account2p12,12), (account2p13,13), (account2p20,20), (account2p21,11), (account2p22,22), (account2p23,13), (deviceId2p10,10), (deviceId2p11,11), (deviceId2p12,12), (deviceId2p13,13), (deviceId2p20,20), (deviceId2p21,11), (deviceId2p22,22), (deviceId2p23,13), (mobile2p10,10), (mobile2p11,11), (mobile2p12,12), (mobile2p13,13), (mobile2p20,20), (mobile2p21,11), (mobile2p22,22), (mobile2p23,13), (account5p10,10), (account5p11,11), (account5p12,12), (account5p13,13), (account5p20,20), (account5p21,11), (account5p22,22), (account5p23,13), (deviceId5p10,10), (deviceId5p11,11), (deviceId5p12,12), (deviceId5p13,13), (deviceId5p20,20), (deviceId5p21,11), (deviceId5p22,22), (deviceId5p23,13), (mobile5p10,10), (mobile5p11,11), (mobile5p12,12), (mobile5p13,13), (mobile5p20,20), (mobile5p21,11), (mobile5p22,22), (mobile5p23,13), (account10p10,10), (account10p11,11), (account10p12,12), (account10p13,13), (account10p20,20), (account10p21,11), (account10p22,22), (account10p23,13), (deviceId10p10,10), (deviceId10p11,11), (deviceId10p12,12), (deviceId10p13,13), (deviceId10p20,20), (deviceId10p21,11), (deviceId10p22,22), (deviceId10p23,13), (mobile10p10,10), (mobile10p11,11), (mobile10p12,12), (mobile10p13,13), (mobile10p20,20), (mobile10p21,11), (mobile10p22,22), (mobile10p23,13), (account60p10,10), (account60p11,11), (account60p12,12), (account60p13,13), (account60p20,20), (account60p21,11), (account60p22,22), (account60p23,13), (deviceId60p10,10), (deviceId60p11,11), (deviceId60p12,12), (deviceId60p13,13), (deviceId60p20,20), (deviceId60p21,11), (deviceId60p22,22), (deviceId60p23,13), (mobile60p10,10), (mobile60p11,11), (mobile60p12,12), (mobile60p13,13), (mobile60p20,20), (mobile60p21,11), (mobile60p22,22), (mobile60p23,13), (account180p10,10), (account180p11,11), (account180p12,12), (account180p13,13), (account180p20,20), (account180p21,11), (account180p22,22), (account180p23,13), (deviceId180p10,10), (deviceId180p11,11), (deviceId180p12,12), (deviceId180p13,13), (deviceId180p20,20), (deviceId180p21,11), (deviceId180p22,22), (deviceId180p23,13), (mobile180p10,10), (mobile180p11,11), (mobile180p12,12), (mobile180p13,13), (mobile180p20,20), (mobile180p21,11), (mobile180p22,22), (mobile180p23,13), (account360p10,10), (account360p11,11), (account360p12,12), (account360p13,13), (account360p20,20), (account360p21,11), (account360p22,22), (account360p23,13), (deviceId360p10,10), (deviceId360p11,11), (deviceId360p12,12), (deviceId360p13,13), (deviceId360p20,20), (deviceId360p21,11), (deviceId360p22,22), (deviceId360p23,13), (mobile360p10,10), (mobile360p11,11), (mobile360p12,12), (mobile360p13,13), (mobile360p20,20), (mobile360p21,11), (mobile360p22,22), (mobile360p23,13), (account3600p10,10), (account3600p11,11), (account3600p12,12), (account3600p13,13), (account3600p20,20), (account3600p21,11), (account3600p22,22), (account3600p23,13), (deviceId3600p10,10), (deviceId3600p11,11), (deviceId3600p12,12), (deviceId3600p13,13), (deviceId3600p20,20), (deviceId3600p21,11), (deviceId3600p22,22), (deviceId3600p23,13), (mobile3600p10,10), (mobile3600p11,11), (mobile3600p12,12), (mobile3600p13,13), (mobile3600p20,20), (mobile3600p21,11), (mobile3600p22,22), (mobile3600p23,13), (account59200p10,10), (account59200p11,11), (account59200p12,12), (account59200p13,13), (account59200p20,20), (account59200p21,11), (account59200p22,22), (account59200p23,13), (deviceId59200p10,10), (deviceId59200p11,11), (deviceId59200p12,12), (deviceId59200p13,13), (deviceId59200p20,20), (deviceId59200p21,11), (deviceId59200p22,22), (deviceId59200p23,13), (mobile59200p10,10), (mobile59200p11,11), (mobile59200p12,12), (mobile59200p13,13), (mobile59200p20,20), (mobile59200p21,11), (mobile59200p22,22), (mobile59200p23,13)))
    (2,List((account2p20,20), (account2p21,11), (account2p22,22), (account2p23,13), (deviceId2p20,20), (deviceId2p21,11), (deviceId2p22,22), (deviceId2p23,13), (mobile2p20,20), (mobile2p21,11), (mobile2p22,22), (mobile2p23,13), (account5p20,20), (account5p21,11), (account5p22,22), (account5p23,13), (deviceId5p20,20), (deviceId5p21,11), (deviceId5p22,22), (deviceId5p23,13), (mobile5p20,20), (mobile5p21,11), (mobile5p22,22), (mobile5p23,13), (account10p20,20), (account10p21,11), (account10p22,22), (account10p23,13), (deviceId10p20,20), (deviceId10p21,11), (deviceId10p22,22), (deviceId10p23,13), (mobile10p20,20), (mobile10p21,11), (mobile10p22,22), (mobile10p23,13), (account60p20,20), (account60p21,11), (account60p22,22), (account60p23,13), (deviceId60p20,20), (deviceId60p21,11), (deviceId60p22,22), (deviceId60p23,13), (mobile60p20,20), (mobile60p21,11), (mobile60p22,22), (mobile60p23,13), (account180p20,20), (account180p21,11), (account180p22,22), (account180p23,13), (deviceId180p20,20), (deviceId180p21,11), (deviceId180p22,22), (deviceId180p23,13), (mobile180p20,20), (mobile180p21,11), (mobile180p22,22), (mobile180p23,13), (account360p20,20), (account360p21,11), (account360p22,22), (account360p23,13), (deviceId360p20,20), (deviceId360p21,11), (deviceId360p22,22), (deviceId360p23,13), (mobile360p20,20), (mobile360p21,11), (mobile360p22,22), (mobile360p23,13), (account3600p20,20), (account3600p21,11), (account3600p22,22), (account3600p23,13), (deviceId3600p20,20), (deviceId3600p21,11), (deviceId3600p22,22), (deviceId3600p23,13), (mobile3600p20,20), (mobile3600p21,11), (mobile3600p22,22), (mobile3600p23,13), (account59200p20,20), (account59200p21,11), (account59200p22,22), (account59200p23,13), (deviceId59200p20,20), (deviceId59200p21,11), (deviceId59200p22,22), (deviceId59200p23,13), (mobile59200p20,20), (mobile59200p21,11), (mobile59200p22,22), (mobile59200p23,13)))
    List转为toMap: RDD[(Int, scala.collection.immutable.Map[String,Int])] = MapPartitionsRDD[204]
    (1,Map(account2p10 -> 10, mobile2p22 -> 22, mobile10p20 -> 20, deviceId59200p10 -> 10, account180p22 -> 22, mobile59200p22 -> 22, account59200p12 -> 12, account3600p12 -> 12, deviceId59200p21 -> 11, deviceId180p23 -> 13, deviceId3600p21 -> 11, account59200p21 -> 11, deviceId3600p10 -> 10, account5p10 -> 10, mobile3600p10 -> 10, account360p20 -> 20, deviceId180p12 -> 12, account360p23 -> 13, mobile59200p11 -> 11, mobile5p12 -> 12, account60p11 -> 11, deviceId10p13 -> 13, mobile180p13 -> 13, account180p12 -> 12, mobile60p21 -> 11, mobile10p13 -> 13, account59200p10 -> 10, deviceId2p22 -> 22, deviceId10p10 -> 10, deviceId10p21 -> 11, account60p23 -> 13, mobile5p23 -> 13, account5p20 -> 20, deviceId60p13 -> 13, deviceId3600p20 -> 20, account3600p23 -> 13, mobile3600p11 -> 11, account360p12 -> 12, deviceId360p22 -> 22, account2p21 -> 11, deviceId360p11 -> 11, deviceId5p20 -> 20, mobile180p23 -> 13, mobile3600p22 -> 22, deviceId2p11 -> 11, mobile3600p13 -> 13, deviceId3600p22 -> 22, deviceId360p21 -> 11, deviceId5p23 -> 13, deviceId3600p11 -> 11, deviceId5p12 -> 12, account3600p10 -> 10, mobile180p12 -> 12, mobile360p20 -> 20, deviceId360p10 -> 10, mobile10p23 -> 13, deviceId59200p11 -> 11, account60p12 -> 12, deviceId5p13 -> 13, deviceId180p20 -> 20, deviceId10p12 -> 12, account2p11 -> 11, mobile59200p10 -> 10, mobile59200p21 -> 11, mobile360p22 -> 22, deviceId60p20 -> 20, account2p22 -> 22, account3600p20 -> 20, deviceId59200p22 -> 22, account60p22 -> 22, account5p11 -> 11, mobile60p11 -> 11, deviceId10p23 -> 13, deviceId10p22 -> 22, deviceId2p10 -> 10, mobile5p20 -> 20, account360p21 -> 11, mobile360p11 -> 11, account10p11 -> 11, deviceId10p11 -> 11, account5p21 -> 11, account10p22 -> 22, mobile180p20 -> 20, account3600p21 -> 11, deviceId60p23 -> 13, account10p20 -> 20, mobile5p13 -> 13, mobile180p22 -> 22, mobile3600p23 -> 13, mobile2p20 -> 20, account180p23 -> 13, mobile60p22 -> 22, mobile3600p12 -> 12, deviceId2p13 -> 13, deviceId2p21 -> 11, mobile10p12 -> 12, account360p10 -> 10, deviceId60p12 -> 12, mobile60p23 -> 13, account10p23 -> 13, account60p13 -> 13, deviceId180p10 -> 10, account180p20 -> 20, mobile180p11 -> 11, mobile2p13 -> 13, mobile59200p20 -> 20, deviceId2p20 -> 20, deviceId180p21 -> 11, deviceId59200p23 -> 13, deviceId60p22 -> 22, mobile360p23 -> 13, account5p22 -> 22, account2p12 -> 12, mobile60p12 -> 12, account60p10 -> 10, mobile10p22 -> 22, deviceId59200p12 -> 12, account10p12 -> 12, mobile360p21 -> 11, account2p23 -> 13, deviceId360p20 -> 20, mobile59200p13 -> 13, mobile5p21 -> 11, deviceId5p22 -> 22, mobile360p12 -> 12, mobile5p10 -> 10, account180p10 -> 10, account60p20 -> 20, account10p21 -> 11, account360p22 -> 22, account5p23 -> 13, account59200p22 -> 22, account180p13 -> 13, deviceId3600p12 -> 12, mobile10p11 -> 11, account360p11 -> 11, account5p13 -> 13, mobile2p23 -> 13, account59200p13 -> 13, deviceId3600p23 -> 13, deviceId5p11 -> 11, account3600p11 -> 11, account10p10 -> 10, account3600p22 -> 22, deviceId360p13 -> 13, mobile180p10 -> 10, account59200p20 -> 20, account2p20 -> 20, mobile60p20 -> 20, deviceId360p12 -> 12, deviceId59200p20 -> 20, mobile10p21 -> 11, account59200p11 -> 11, mobile180p21 -> 11, mobile2p10 -> 10, deviceId180p22 -> 22, deviceId360p23 -> 13, deviceId2p23 -> 13, account3600p13 -> 13, mobile2p12 -> 12, deviceId180p11 -> 11, mobile60p13 -> 13, mobile2p21 -> 11, mobile3600p20 -> 20, deviceId59200p13 -> 13, account180p21 -> 11, account10p13 -> 13, deviceId2p12 -> 12, account2p13 -> 13, deviceId10p20 -> 20, deviceId180p13 -> 13, mobile3600p21 -> 11, mobile10p10 -> 10, account59200p23 -> 13, deviceId60p11 -> 11, account60p21 -> 11, mobile360p13 -> 13, mobile5p22 -> 22, deviceId60p10 -> 10, account180p11 -> 11, deviceId60p21 -> 11, mobile59200p12 -> 12, deviceId5p21 -> 11, mobile5p11 -> 11, mobile2p11 -> 11, mobile60p10 -> 10, account5p12 -> 12, account360p13 -> 13, deviceId3600p13 -> 13, mobile360p10 -> 10, mobile59200p23 -> 13, deviceId5p10 -> 10))
    (2,Map(mobile2p22 -> 22, mobile10p20 -> 20, account180p22 -> 22, mobile59200p22 -> 22, deviceId59200p21 -> 11, deviceId180p23 -> 13, deviceId3600p21 -> 11, account59200p21 -> 11, account360p20 -> 20, account360p23 -> 13, mobile60p21 -> 11, deviceId2p22 -> 22, deviceId10p21 -> 11, account60p23 -> 13, mobile5p23 -> 13, account5p20 -> 20, deviceId3600p20 -> 20, account3600p23 -> 13, deviceId360p22 -> 22, account2p21 -> 11, deviceId5p20 -> 20, mobile180p23 -> 13, mobile3600p22 -> 22, deviceId3600p22 -> 22, deviceId360p21 -> 11, deviceId5p23 -> 13, mobile360p20 -> 20, mobile10p23 -> 13, deviceId180p20 -> 20, mobile59200p21 -> 11, mobile360p22 -> 22, deviceId60p20 -> 20, account2p22 -> 22, account3600p20 -> 20, deviceId59200p22 -> 22, account60p22 -> 22, deviceId10p23 -> 13, deviceId10p22 -> 22, mobile5p20 -> 20, account360p21 -> 11, account5p21 -> 11, account10p22 -> 22, mobile180p20 -> 20, account3600p21 -> 11, deviceId60p23 -> 13, account10p20 -> 20, mobile180p22 -> 22, mobile3600p23 -> 13, mobile2p20 -> 20, account180p23 -> 13, mobile60p22 -> 22, deviceId2p21 -> 11, mobile60p23 -> 13, account10p23 -> 13, account180p20 -> 20, mobile59200p20 -> 20, deviceId2p20 -> 20, deviceId180p21 -> 11, deviceId59200p23 -> 13, deviceId60p22 -> 22, mobile360p23 -> 13, account5p22 -> 22, mobile10p22 -> 22, mobile360p21 -> 11, account2p23 -> 13, deviceId360p20 -> 20, mobile5p21 -> 11, deviceId5p22 -> 22, account60p20 -> 20, account10p21 -> 11, account360p22 -> 22, account5p23 -> 13, account59200p22 -> 22, mobile2p23 -> 13, deviceId3600p23 -> 13, account3600p22 -> 22, account59200p20 -> 20, account2p20 -> 20, mobile60p20 -> 20, deviceId59200p20 -> 20, mobile10p21 -> 11, mobile180p21 -> 11, deviceId180p22 -> 22, deviceId360p23 -> 13, deviceId2p23 -> 13, mobile2p21 -> 11, mobile3600p20 -> 20, account180p21 -> 11, deviceId10p20 -> 20, mobile3600p21 -> 11, account59200p23 -> 13, account60p21 -> 11, mobile5p22 -> 22, deviceId60p21 -> 11, deviceId5p21 -> 11, mobile59200p23 -> 13))
      */
    val flatRdd = mergeRdd.map(p => {
      val id = p._1
      val iterable = p._2

      (id, iterable.toList.flatten)
      (id, iterable.toList.flatten.toMap)
    })
    flatRdd.collect.foreach(println)


    //**********reduceByKey**********
    val result = unionRdd.reduceByKey((a, b) => {
      a ++ b
    })
    //(2,Map(mobile2p22 -> 22, mobile10p20 -> 20, account180p22 -> 22, mobile59200p22 -> 22, deviceId59200p21 -> 11, deviceId180p23 -> 13, deviceId3600p21 -> 11, account59200p21 -> 11, account360p20 -> 20, account360p23 -> 13, mobile60p21 -> 11, deviceId2p22 -> 22, deviceId10p21 -> 11, account60p23 -> 13, mobile5p23 -> 13, account5p20 -> 20, deviceId3600p20 -> 20, account3600p23 -> 13, deviceId360p22 -> 22, account2p21 -> 11, deviceId5p20 -> 20, mobile180p23 -> 13, mobile3600p22 -> 22, deviceId3600p22 -> 22, deviceId360p21 -> 11, deviceId5p23 -> 13, mobile360p20 -> 20, mobile10p23 -> 13, deviceId180p20 -> 20, mobile59200p21 -> 11, mobile360p22 -> 22, deviceId60p20 -> 20, account2p22 -> 22, account3600p20 -> 20, deviceId59200p22 -> 22, account60p22 -> 22, deviceId10p23 -> 13, deviceId10p22 -> 22, mobile5p20 -> 20, account360p21 -> 11, account5p21 -> 11, account10p22 -> 22, mobile180p20 -> 20, account3600p21 -> 11, deviceId60p23 -> 13, account10p20 -> 20, mobile180p22 -> 22, mobile3600p23 -> 13, mobile2p20 -> 20, account180p23 -> 13, mobile60p22 -> 22, deviceId2p21 -> 11, mobile60p23 -> 13, account10p23 -> 13, account180p20 -> 20, mobile59200p20 -> 20, deviceId2p20 -> 20, deviceId180p21 -> 11, deviceId59200p23 -> 13, deviceId60p22 -> 22, mobile360p23 -> 13, account5p22 -> 22, mobile10p22 -> 22, mobile360p21 -> 11, account2p23 -> 13, deviceId360p20 -> 20, mobile5p21 -> 11, deviceId5p22 -> 22, account60p20 -> 20, account10p21 -> 11, account360p22 -> 22, account5p23 -> 13, account59200p22 -> 22, mobile2p23 -> 13, deviceId3600p23 -> 13, account3600p22 -> 22, account59200p20 -> 20, account2p20 -> 20, mobile60p20 -> 20, deviceId59200p20 -> 20, mobile10p21 -> 11, mobile180p21 -> 11, deviceId180p22 -> 22, deviceId360p23 -> 13, deviceId2p23 -> 13, mobile2p21 -> 11, mobile3600p20 -> 20, account180p21 -> 11, deviceId10p20 -> 20, mobile3600p21 -> 11, account59200p23 -> 13, account60p21 -> 11, mobile5p22 -> 22, deviceId60p21 -> 11, deviceId5p21 -> 11, mobile59200p23 -> 13))
    result.collect.foreach(println)


    //------------------------------------------------------------

    /**
     * Map里面放的是指标, 指标的数量不会很多. 对于一个维度而言,最多就300个.
     * 而RDD里放的是所有记录,记录有千万级别. 最好不要把这样的数据放到数据结构如List,Map中.而是用RDD自身来管理
     * (2,Map(account180p22 -> 22, account59200p21 -> 11, account360p20 -> 20, account360p23 -> 13, account60p23 -> 13, account5p20 -> 20, account3600p23 -> 13, account2p21 -> 11, account2p22 -> 22, account3600p20 -> 20, account60p22 -> 22, account360p21 -> 11, account5p21 -> 11, account10p22 -> 22, account3600p21 -> 11, account10p20 -> 20, account180p23 -> 13, account10p23 -> 13, account180p20 -> 20, account5p22 -> 22, account2p23 -> 13, account60p20 -> 20, account10p21 -> 11, account360p22 -> 22, account5p23 -> 13, account59200p22 -> 22, account3600p22 -> 22, account59200p20 -> 20, account2p20 -> 20, account180p21 -> 11, account59200p23 -> 13, account60p21 -> 11),
     * Map(deviceId59200p21 -> 11, deviceId180p23 -> 13, deviceId3600p21 -> 11, deviceId2p22 -> 22, deviceId10p21 -> 11, deviceId3600p20 -> 20, deviceId360p22 -> 22, deviceId5p20 -> 20, deviceId3600p22 -> 22, deviceId360p21 -> 11, deviceId5p23 -> 13, deviceId180p20 -> 20, deviceId60p20 -> 20, deviceId59200p22 -> 22, deviceId10p23 -> 13, deviceId10p22 -> 22, deviceId60p23 -> 13, deviceId2p21 -> 11, deviceId2p20 -> 20, deviceId180p21 -> 11, deviceId59200p23 -> 13, deviceId60p22 -> 22, deviceId360p20 -> 20, deviceId5p22 -> 22, deviceId3600p23 -> 13, deviceId59200p20 -> 20, deviceId180p22 -> 22, deviceId360p23 -> 13, deviceId2p23 -> 13, deviceId10p20 -> 20, deviceId60p21 -> 11, deviceId5p21 -> 11),
     * Map(mobile2p22 -> 22, mobile10p20 -> 20, mobile59200p22 -> 22, mobile60p21 -> 11, mobile5p23 -> 13, mobile180p23 -> 13, mobile3600p22 -> 22, mobile360p20 -> 20, mobile10p23 -> 13, mobile59200p21 -> 11, mobile360p22 -> 22, mobile5p20 -> 20, mobile180p20 -> 20, mobile180p22 -> 22, mobile3600p23 -> 13, mobile2p20 -> 20, mobile60p22 -> 22, mobile60p23 -> 13, mobile59200p20 -> 20, mobile360p23 -> 13, mobile10p22 -> 22, mobile360p21 -> 11, mobile5p21 -> 11, mobile2p23 -> 13, mobile60p20 -> 20, mobile10p21 -> 11, mobile180p21 -> 11, mobile2p21 -> 11, mobile3600p20 -> 20, mobile3600p21 -> 11, mobile5p22 -> 22, mobile59200p23 -> 13))
     */
    //将相同维度的放在一起map里. 这样一条记录如果有4个维度, 就有4个map
    //下面的map声明放在外面(mergeRdd)会不会有问题?
    //放在外面的话,当RDD里的第一条记录执行完毕.map里已经有数据了. 那么当执行第二条记录时. map不是会在原来的基础上添加吗?
    var accountMap = scala.collection.immutable.Map[String, Int]()
    var deviceIdMap = scala.collection.immutable.Map[String, Int]()
    var mobileMap = scala.collection.immutable.Map[String, Int]()
    val flatRdd2 = mergeRdd.map(p => {
      val id = p._1
      val iterable = p._2

      //注意:在集群上运行spark-shell的控制台并不会打印下面的语句.
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + id + ":::(PRE)MAPSIZE:::" + accountMap.size) //0

      val map = iterable.toList.flatten.toMap
      map.map(m => {
        if (m._1.startsWith("account")) {
          accountMap += m._1 -> m._2
        } else if (m._1.startsWith("deviceId")) {
          deviceIdMap += m._1 -> m._2
        } else if (m._1.startsWith("mobile")) {
          mobileMap += m._1 -> m._2
        }
      })

      //makeRdd模拟了4条记录,一共3+5=8个指标. 每个维度(共三个维度)都有4*8=32个
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + id + ":::(POST)MAPSIZE:::" + accountMap.size) //32
      (id, accountMap, deviceIdMap, mobileMap)
    })
    flatRdd2.count
    flatRdd2.collect.foreach(println)
    accountMap.size //本地模式和集群模式,结果都为0

    flatRdd2.partitions.size // 分区数

    //上面基于groupByKey的mergeRdd,能不能使用reduceByKey
    //不行,因为reduce的要求是两个相同的对象,最终合并为一个对象. 而我们的结果是由原先的一个Map,根据不同的key,加入到不同的map中.
  }

  //-----------------------------------------
  //spark中也有可能出现数据倾斜问题(如join等，当key有大部分相同时(如像hive数据倾斜那样join的字段很多为null))，所以需要查看各分区的元素数目来判断数据各分区分布情况
  def testPartitionNumber(): Unit ={
    def getPartitionCounts[T](rdd : RDD[T]) : Array[Long] = {
      sc.runJob(rdd, getIteratorSize _)
    }
    def getIteratorSize[T](iterator: Iterator[T]): Long = {
      var count = 0L
      while (iterator.hasNext) {
        count += 1L
        iterator.next()
      }
      count
    }

    val rdd = sc.parallelize(Array(("A",1),("A",1),("A",1),("A",1),("A",1)), 2)
    getPartitionCounts(rdd).foreach(println)
  }

  //优化策略: http://blogx.github.io/how-to-tune-your-apache-spark-jobs-part-1.html
  import org.apache.spark.rdd.RDD

  //Action的执行时间. 调用collect或者其他比如count方法都可以
  def actionTime[T](rdd : RDD[T]): Unit ={
    val start = System.currentTimeMillis()
    rdd.collect()
    println((System.currentTimeMillis() - start)/1000)
  }
  //每个分区中总的元素个数
  def rddStaticCount[T](rdd1 : RDD[T]): Unit ={
    rdd1.mapPartitionsWithIndex{
      (partIdx,iter) => {
        //每个partition的元素个数
        var part_map = scala.collection.mutable.Map[String,Int]()
        while(iter.hasNext){
          var part_name = "part_" + partIdx;
          if(part_map.contains(part_name)) {
            //如果map中的partition名称已经存在, 则计数器+1
            var ele_cnt = part_map(part_name)
            part_map(part_name) = ele_cnt + 1
          } else {
            //如果不存在, 那么这个分区的元素个数初始值为1. 典型的没有新建初值为1,有加1
            part_map(part_name) = 1
          }
          //计算总数时, 不关心里面的内容, 但是必须要调用一次的!
          iter.next()
        }
        //对结果Map进行迭代,作为返回值,才能调用下面的collect输出
        part_map.iterator
      }
    }.collect.foreach(println)
  }
  //RDD[(String,Int)]中都有哪些key分布. 如果是RDD[String]这样的,我们一般不关心里面的值.正如我们不关心RDD[(String,Int)]中的Int
  def rddStaticMapKeys(someRdd3 : RDD[(String,Int)]): Unit ={
    someRdd3.mapPartitionsWithIndex{
      //每个分区的迭代器内容是属于这个分区的元素. 根据你具体保存的元素类型是有关系的.
      //比如我们在RDD里保存的是一个Map. 为了统计Map中分布的key. 在方法的参数中必须传入具体的类型.
      (partIdx,iter) => {
        //Map的key为partitionName, Map的Value是一个Set, 包含了这个分区中所有的key
        //由于我们要获取的是Map中的key, 而它的类型是跟RDD[(String,Int)]相关的,即String
        var part_map = scala.collection.mutable.Map[String,Set[String]]()
        while(iter.hasNext){
          var part_name = "part_" + partIdx;
          //elem是一个Map: (String,Int)
          var elem = iter.next()
          if(part_map.contains(part_name)) {
            var elems = part_map(part_name)
            //取Map的key, 然后使用Set的+=方法加入到Set不可重复集合中
            elems += elem._1
            part_map(part_name) = elems
          } else {
            //一个都不存在的情况下,创建一个Set,并把当前key加入到集合中
            part_map(part_name) = Set[String](elem._1)
          }
        }
        part_map.iterator
      }
    }.collect.foreach(println)
  }

  def makeMapRdd(part : Int=2): RDD[(String,Int)] ={
    val keys = List("a","b","c","d","e","f","g","h","i","j")
    val large = Range(1,1000000)
    val lst = large.map(k=>{
      val key = keys(scala.util.Random.nextInt(keys.size))
      key->scala.util.Random.nextInt(k)
    })
    sc.parallelize(lst, part)
  }

  def testOptimize(){
    val rddY = makeMapRdd()

    //优化点1: 组合规约时,避免使用groupByKey
    val groupByRdd = rddY.groupByKey().mapValues(_.sum)
    val reduceByRdd = rddY.reduceByKey(_ + _)

    //优化点2: 输入输出类型不同时,避免使用reduceByKey
    //Array[(String, scala.collection.immutable.Set[Int])]
    val reduceSetRdd = rddY.map(kv => (kv._1, Set[Int]() + kv._2)).reduceByKey(_++_)

    val zero = Set[Int]()
    val seqOp = (set : Set[Int], v : Int) => set + v
    val comOp = (set1 : Set[Int], set2 : Set[Int]) => set1 ++ set2
    val aggRdd = rddY.aggregateByKey(zero)(seqOp, comOp)

    actionTime(groupByRdd)
    actionTime(reduceByRdd)
    actionTime(reduceSetRdd)
    actionTime(aggRdd)
  }

  def testShuffle(): Unit ={
    //不同分区的MapTask, reduceByKey使用相同的分区数, 最后join时,没有shuffle数据
    val someRdd1 = makeMapRdd(4)
    val reduceRdd1 = someRdd1.reduceByKey(_+_, 3)
    rddStaticCount(someRdd1)
    rddStaticMapKeys(someRdd1)

    val someOtherRdd1 = makeMapRdd(2)
    val reduceRdd2 = someOtherRdd1.reduceByKey(_+_, 3)
    val joinRdd1 = reduceRdd1.join(reduceRdd2)
    actionTime(joinRdd1) //7s

    //不同分区的MapTask, reduceByKey使用不同的分区数, 最后join时,有shuffle数据
    val someRdd2 = makeMapRdd(4)
    val reduceRdd3 = someRdd2.reduceByKey(_+_, 2)

    val someOtherRdd2 = makeMapRdd(2)
    val reduceRdd4 = someOtherRdd2.reduceByKey(_+_, 3)
    val joinRdd2 = reduceRdd3.join(reduceRdd4)
    actionTime(joinRdd2) //7s
  }

}