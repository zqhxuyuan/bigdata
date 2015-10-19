package sparkly.math.testing

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

  val cores = 4
  var conf = new SparkConf().setMaster(s"local[$cores]").setAppName(getClass.getName)

  @transient var sc: SparkContext = _

  override def beforeEach() {
    sc = new SparkContext(conf)
    super.beforeEach()
  }

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext() = {
    if (sc != null) {
      sc.stop()
    }

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    sc = null
  }

}