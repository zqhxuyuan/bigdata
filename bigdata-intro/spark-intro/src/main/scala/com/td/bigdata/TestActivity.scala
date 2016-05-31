package com.td.bigdata.crosspartner.test

import com.td.bigdata.crosspartner.Constant
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.ScanResult

/**
  * Created by zhengqh on 16/2/18.
  */
object TestActivity extends App{

  val conf = new SparkConf().setMaster("local").setAppName("LoanTest")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)
  import sqlContext.implicits._

  def filterTongdunData(): Unit ={
    var activityDf = sc.parallelize(Constant.testData, 2).toDF
    activityDf = activityDf.filter(!activityDf("partner_code").equalTo("tongdun_data"))
    activityDf.show()
  }

  def testLocal(): Unit ={
    sc.parallelize(Constant.testData).foreachPartition(partitionOfRecords=>{
      val jedis = Constant.getRedisInstant(Constant.ENV_LOCAL)
      partitionOfRecords.foreach(record=>{
        jedis.eval(Constant.script, 1, record.attribute, ""+record.timestamp, record.partner)
      })
      jedis.close()
    })
  }

  def testDevelop(): Unit ={
    sc.parallelize(Constant.testData).foreachPartition(partitionOfRecords=>{
      val jedis = Constant.getRedisInstant(Constant.ENV_DEV)
      partitionOfRecords.foreach(record=>{
        jedis.eval(Constant.script, 1, record.attribute, ""+record.timestamp, record.partner)
      })
      jedis.close()
    })
  }

}
