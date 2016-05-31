package com.td.bigdata.crosspartner.test

import com.td.bigdata.crosspartner.Constant
import redis.clients.jedis.ScanResult

/**
  * Created by zhengqh on 16/2/18.
  *
  * scp target/cross-partner-1.0-SNAPSHOT-jar-with-dependencies.jar qihuang.zheng@fort.tongdun.cn:192.168.47.211/admin/tmp
  * sudo mv /tmp/cross-partner-1.0-SNAPSHOT-jar-with-dependencies.jar ~/ && sudo chown qihuang.zheng:users cross-partner-1.0-SNAPSHOT-jar-with-dependencies.jar

  * java -cp cross-partner-1.0-SNAPSHOT-jar-with-dependencies.jar com.td.bigdata.crosspartner.test.TestRedis
  */
object TestRedis {

  def deleteOneMember(): Unit ={
    val jedis = Constant.getRedisInstant(Constant.ENV_LOCAL)
    val testKey = "mob_15959086950"

    // 模拟添加一条要删除的member
    jedis.zadd(testKey, 11111111113L, "tongdun_data")
    var res1 = jedis.zrange(testKey, 0, -1)
    res1.toArray.foreach(println)
    println("-------------------")

    // 手工删除一条记录
    jedis.zrem(testKey, "tongdun_data")

    // 删除后验证数据
    res1 = jedis.zrange(testKey, 0, -1)
    res1.toArray.foreach(println)

    jedis.close()
  }

  def deleteAllMember(): Unit ={
    val jedis = Constant.getRedisInstant(Constant.ENV_PROD)

    // 遍历所有key删除
    var scanRet = "0"
    var count = 0
    do {
      val ret: ScanResult[String] = jedis.scan(scanRet)
      scanRet = ret.getStringCursor()
      val result = ret.getResult
      val iterator = result.iterator()
      while(iterator.hasNext){
        val key = iterator.next()
        //不能直接对所有的key进行同一个zrem操作,因为系统中已有的key的类型并不一定都是zset
        if(jedis.`type`(key).equals("zset")){
          jedis.zrem(key, "tongdun_data")
          count  = count + 1
        }
      }
    } while (!scanRet.equals("0"))

    println("clean size:"+count)
    jedis.close()
  }

  def main(args: Array[String]) {
    val start = System.currentTimeMillis()

    deleteAllMember()

    val end = System.currentTimeMillis()
    println("END...." + (end-start)/1000)
  }
}
