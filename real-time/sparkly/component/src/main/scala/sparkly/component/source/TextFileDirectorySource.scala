package sparkly.component.source

import sparkly.core._
import sparkly.core.PropertyType._
import org.apache.spark.streaming.dstream.DStream
import sparkly.core.OutputStreamMetadata
import sparkly.core.Context
import sparkly.core.ComponentMetadata
import sparkly.core.PropertyMetadata
import scala.Some
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.commons.io.FilenameUtils


class TextFileDirectorySource extends Component {

  override def metadata = ComponentMetadata(
    name = "Text files source", description = "Monitor a directory and process text files created in it (files written in nested directories not supported). Note that file system should be compatible with the HDFS API (that is, HDFS, S3, NFS, etc.).",
    category = "Data Sources",
    outputs = Map(
      "Instances" -> OutputStreamMetadata(namedFeatures = Map("Line" -> FeatureType.STRING))
    ),
    properties = Map(
      "Directory" -> PropertyMetadata(STRING),
      "Filename pattern" -> PropertyMetadata(STRING, defaultValue = Some("*"))
    )
  )

  override def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val directory = context.property("Directory").as[String]
    val filter = context.property("Filename pattern").selectedValue match {
      case None => (path: Path) => true
      case Some("") => (path: Path) => true
      case Some(pattern) => (path: Path) => FilenameUtils.wildcardMatch(path.getName, pattern)
    }

    val featureName = context.outputMappers("Instances").featureName("Line")

    val lineStream = context.ssc
      .fileStream[LongWritable, Text, TextInputFormat](directory, filter, false)
      .map(_._2.toString)
      .map(line => Instance(featureName -> line))

    Map("Instances" -> lineStream)
  }

}
