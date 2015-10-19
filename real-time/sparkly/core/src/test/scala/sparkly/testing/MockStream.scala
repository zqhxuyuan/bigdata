package sparkly.testing

import com.google.common.util.concurrent.RateLimiter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import sparkly.core._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}


import ExecutionContext.Implicits.global

class MockStream extends Component {

  var componentId: String = null

  private val queue = new mutable.SynchronizedQueue[RDD[Instance]]()
  private var sc: Option[SparkContext] = None

  override def metadata: ComponentMetadata = ComponentMetadata("Fake input stream")

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    componentId = context.componentId
    sc = Some(context.ssc.sparkContext)
    Map(MockStream.OUTPUT_NAME ->  context.ssc.queueStream(queue, oneAtATime = false))
  }

  def push(instances: Instance*): Unit = push(instances.toList)

  def push(instances: List[Instance]): Unit = sc match{
    case Some(sparkContext) => queue += sparkContext.makeRDD(instances)
    case None => throw new IllegalStateException("You can't push data before the fake input stream is initiated")
  }

  def push(rate: Int, generator: () => Instance): Unit = Future {
    val (limiter, batchSize) = createRateLimiter(rate)

    while(true) {
      limiter.acquire()
      val instances = (1 to batchSize).map(i => generator()).toList
      push(instances)
    }
  }

  def push(rate: Int, it: Iterator[Instance]): Unit = Future {
    val (limiter, batchSize) = createRateLimiter(rate)

    while(it.hasNext) {
      limiter.acquire()
      val instances = (1 to batchSize).flatMap(i => if(it.hasNext) Some(it.next()) else None).toList
      if(!instances.isEmpty) {
        push(instances)
      }
    }
  }

  private def createRateLimiter(rate: Int): (RateLimiter, Int) = {
    val batchDurationSeconds = ComponentSpec.batchDurationMs.toDouble / 1000.0
    val batchSize = (rate.toDouble * ComponentSpec.batchDurationMs.toDouble / 1000.0).toInt
    val limiter = RateLimiter.create(1 / batchDurationSeconds)
    (limiter, batchSize)
  }

}

object MockStream {
  val OUTPUT_NAME = "output"
}
