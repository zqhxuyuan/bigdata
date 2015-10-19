package sparkly.config

import java.io.File

import com.typesafe.config.ConfigFactory

object SparklyConfig {

  val WEB_CONFIG = ConfigFactory.load().getConfig("web")
  val HOSTNAME = WEB_CONFIG.getString("hostname")
  val WEB_PORT = WEB_CONFIG.getInt("port")
  val WEB_SOURCES = WEB_CONFIG.getString("sources")

  val DATA_CONFIG = ConfigFactory.load().getConfig("data")
  val BASE_LOCAL_DIRECTORY = DATA_CONFIG.getString("base-local-directory")
  val BASE_DISTRIBUTED_DIRECTORY = DATA_CONFIG.getString("base-distributed-directory")

  val DB_DIRECTORY = new File(BASE_LOCAL_DIRECTORY, "db").getAbsolutePath
}
