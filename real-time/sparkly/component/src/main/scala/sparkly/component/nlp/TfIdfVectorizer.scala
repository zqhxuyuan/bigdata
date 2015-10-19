package sparkly.component.nlp

import org.apache.spark.streaming.dstream.DStream
import sparkly.core._
import sparkly.core.PropertyType._
import scala.Some

class TfIdfVectorizer extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "TF-IDF Vectorizer", category = "Pre-processor",
    description = "Convert a text to a vector of TF-IDF features.",
    inputs = Map ("Input" -> InputStreamMetadata(namedFeatures = Map("Text" -> FeatureType.STRING))),
    outputs = Map("Output" -> OutputStreamMetadata(from = Some("Input"), namedFeatures = Map("Features" -> FeatureType.VECTOR))),
    properties = Map (
      "Vocabulary size" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(5000)),
      "Minimum frequency" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(0.0), description = "Terms will be ignored if they don't occur in more than a minimum number of documents (as a %)"),
      "Language" -> PropertyMetadata(PropertyType.STRING, defaultValue = Some("English"), mandatory = false, description = "Language used to extract terms", acceptedValues = AnalyzerFactory.availableLanguages),
      "Min n-gram" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(1), description = "The smallest n-gram to generate"),
      "Max n-gram" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(1), description = "The largest n-gram to generate"),
      "Ignore pattern" -> PropertyMetadata(PropertyType.STRING, mandatory = false, description = "Terms following this pattern will be ignored"),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to avoid repartitioning.")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val vocabularySize = context.property("Vocabulary size").as[Int]
    val minDocFreq = context.property("Minimum frequency").as[Double]
    var model = TfIdfModel(vocabularySize, minDocFreq)

    val language = context.property("Language").as[String]
    val minNGram = context.property("Min n-gram").as[Int]
    val maxNGram = context.property("Max n-gram").as[Int]
    val ignorePattern = context.property("Ignore pattern").or("")
    val tokenizer = TextTokenizer(language, minNGram, maxNGram, ignorePattern)

    val partitions = context.property("Parallelism").as[Int]
    val stream = if(partitions < 1) context.dstream("Input", "Output") else context.dstream("Input", "Output").repartition(partitions)

    val terms = stream.map { instance =>
      val text = instance.inputFeature("Text").asString
      (instance, tokenizer.tokenize(text))
    }

    // Add vocabulary
    terms.foreachRDD{ rdd =>
      model = rdd.map(_._2).treeAggregate(model)(
        seqOp = (model: TfIdfModel, terms: List[String]) => model.add(terms),
        combOp = _ + _,
        depth = 2
      )
    }

    // Vectorize
    val out = terms.map{ case(instance, terms) =>
      val features = model.tfIdf(terms)
      instance.outputFeature("Features", features)
    }

    Map("Output" -> out)
  }

}