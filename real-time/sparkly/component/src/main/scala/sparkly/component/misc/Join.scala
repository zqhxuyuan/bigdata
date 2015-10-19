package sparkly.component.misc

import sparkly.core._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.PropertyType.{INTEGER, STRING}

class Join extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Join", category = "Miscellaneous",
    description = "Join data from 2 streams, based on common fields between them. This component only apply join within each batch that comes from the input streams. It joins 'Stream 1' and 'Stream 2' together using 'Join features'. Output features should contains (in order) 'Join features', 'Non-Join features' from 'Stream 1' then 'Non-join features' from 'Stream 2'.",
    properties = Map(
      "Type" -> PropertyMetadata(STRING, defaultValue = Some("Inner join"), acceptedValues = List("Inner join", "Left join", "Right join")),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    ),
    inputs = Map (
      "Stream 1" -> InputStreamMetadata(listedFeatures = Map("Join features" -> FeatureType.ANY, "Non-join features" -> FeatureType.ANY)),
      "Stream 2" -> InputStreamMetadata(listedFeatures = Map("Join features" -> FeatureType.ANY, "Non-join features" -> FeatureType.ANY))
    ),
    outputs = Map("Output" -> OutputStreamMetadata(listedFeatures = Map("Join and Non-join features" -> FeatureType.ANY)))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val joinType = context.property("Type").as[String]
    val outputMapper = context.outputMappers("Output")
    val leftFeatureSize = context.inputSize("Stream 1", "Non-join features")
    val rightFeatureSize = context.inputSize("Stream 2", "Non-join features")
    val parallelism = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)

    val stream1 = context.dstream("Stream 1").map(instance => (instance.inputFeatures("Join features"), instance.inputFeatures("Non-join features")))
    val stream2 = context.dstream("Stream 2").map(instance => (instance.inputFeatures("Join features"), instance.inputFeatures("Non-join features")))

    def emptyFeatureList(size: Int): FeatureList = FeatureList(List.fill(size)(Feature(None)))

    val joinedStream = joinType match {
      case "Inner join" => stream1.join(stream2, numPartitions = parallelism).map(data => (data._1, data._2._1, data._2._2))
      case "Left join" => stream1.leftOuterJoin(stream2, numPartitions = parallelism).map(data => (data._1, data._2._1, data._2._2.getOrElse(emptyFeatureList(rightFeatureSize))))
      case "Right join" => stream1.rightOuterJoin(stream2, numPartitions = parallelism).map(data => (data._1, data._2._1.getOrElse(emptyFeatureList(leftFeatureSize)), data._2._2))
    }

    val outputStream = joinedStream.map{ case (joinFeatures: FeatureList, nonJoinFeatures1: FeatureList, nonJoinFeatures2: FeatureList) =>
      val features = joinFeatures.asRawOr(null) ++ nonJoinFeatures1.asRawOr(null) ++ nonJoinFeatures2.asRawOr(null)
      val namedFeatures = outputMapper.featuresNames("Join and Non-join features") zip features
      new Instance(namedFeatures.toMap)
    }

    Map("Output" -> outputStream)
  }
}
