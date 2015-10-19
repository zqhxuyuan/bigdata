package sparkly.component.source

import sparkly.core._
import sparkly.core.PropertyType._
import org.apache.spark.streaming.dstream.DStream
import scala.Some
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import sparkly.core.OutputStreamMetadata
import sparkly.core.Context
import sparkly.core.ComponentMetadata
import sparkly.core.PropertyMetadata
import scala.Some


class CsvFileDirectorySource extends Component {

  override def metadata = ComponentMetadata(
    name = "Csv files source", description = "Monitor a directory and parse csv files created in it (files written in nested directories not supported). Note that file system should be compatible with the HDFS API (that is, HDFS, S3, NFS, etc.).",
    category = "Data Sources",
    outputs = Map(
      "Instances" -> OutputStreamMetadata(listedFeatures = Map("Features" -> FeatureType.STRING))
    ),
    properties = Map(
      "Directory" -> PropertyMetadata(STRING),
      "Filename pattern" -> PropertyMetadata(STRING, defaultValue = Some("*.csv")),
      "Separator" -> PropertyMetadata(STRING, defaultValue = Some(";"), acceptedValues = List(";", ","))
    )
  )

  override def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val directory = context.property("Directory").as[String]
    val separator = context.properties("Separator").as[String]
    val filter = context.property("Filename pattern").selectedValue match {
      case None => (path: Path) => true
      case Some("") => (path: Path) => true
      case Some(pattern) => (path: Path) => FilenameUtils.wildcardMatch(path.getName, pattern)
    }

    val featureNames = context.outputMappers("Instances").featuresNames("Features")

    val output = context.ssc
      .fileStream[LongWritable, Text, TextInputFormat](directory, filter, false)
      .map(_._2.toString)
      .map(line => line.split(separator).toList)
      .map(values => {
      val entries = values.zipWithIndex
        .filter { case (value, index) => index < featureNames.size}
        .map { case (value, index) => featureNames(index) -> value}
        .toMap

      Instance(entries)
    })

    Map("Instances" -> output)
  }

}
