package com.td.bigdata.spark.clickcount

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisClientNoSer extends Serializable {
  val redisHost = "192.168.6.52"
  val redisPort = 6379
  val redisTimeout = 30000
  val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}
