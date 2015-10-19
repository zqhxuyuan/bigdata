package sparkly.testing

import org.apache.spark.streaming.dstream.DStream
import sparkly.core._

class TestClassifier extends Component {
  def metadata = ComponentMetadata("Test classifier", "Only for test purpose")
  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = ???
}

class TestComponent extends Component {
  def metadata = ComponentMetadata("Test component", "Only for test purpose")
  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = ???
}
