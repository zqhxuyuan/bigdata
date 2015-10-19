package com.github.bbiletskyy.fraud

/**
 * Created by zhengqh on 15/10/12.
 */
object Main {
  def main(args: Array[String]): Unit = {
    val driverHost = "localhost"
    val driverPort = 7777
    val receiverActorName = "receiver"
    val actorSystem = Spark.init(driverHost, driverPort, receiverActorName)
    Rest.init(actorSystem, driverHost, driverPort, receiverActorName)
  }
}