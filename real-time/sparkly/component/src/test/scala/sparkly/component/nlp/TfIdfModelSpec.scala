package sparkly.component.nlp

import breeze.linalg.functions._
import org.scalatest._
import sparkly.component.source.dataset.ReutersDataset

class TfIdfModelSpec extends FlatSpec with Matchers {

  val tokenizer = TextTokenizer(language = "English", ignorePattern = "[1-9](\\w+)*")

  "TfIdfModel" should "compute features frequencies from text" in {
    // Given
    val model = TfIdfModel(20)
    val terms = List("I", "do", "what", "I", "want", "to", "do", "now")

    // When
    val features = model.tf(terms)

    // Then
    val frequencies = features.toArray
    frequencies.count(_ == 0.125) should be (4) // what, want, to, now
    frequencies.count(_ == 0.25) should be (2) // I, do
    frequencies.sum should be (1.0)
  }

  "TfIdfModel" should "compute features frequencies consistently" in {
    // Given
    val terms = List("I", "do", "what", "I", "want", "to", "do")
    val model1 = TfIdfModel(20)
    val model2 = TfIdfModel(20)

    // When
    val features1 = model1.tf(terms)
    val features2 = model2.tf(terms)

    // Then
    features1 should be (features2)
  }

  "TfIdfModel" should "support minimum term frequency" in {
    // Given
    var model = TfIdfModel(20, minDocFreq = 0.5)
    val documents = List (
      List("my", "cat", "looks", "weird"),
      List("your", "umbrella", "looks", "good"),
      List("it", "feels", "good"),
      List("any", "cat", "should", "see", "it")
    )

    // When
    documents.foreach { terms =>
      model = model.add(terms)
    }

    val features1 = model.tfIdf(documents(0))
    val features2 = model.tfIdf(documents(1))
    val features3 = model.tfIdf(documents(2))
    val features4 = model.tfIdf(documents(3))

    // Then
    features1.toArray.count(_ != 0.0) should be (2) // looks, cat
    features2.toArray.count(_ != 0.0) should be (2) // looks, good
    features3.toArray.count(_ != 0.0) should be (2) // it, good
    features4.toArray.count(_ != 0.0) should be (2) // cat, it
  }

  "TfIdfModel" should "compute tf-idf" in {
    // Given
    var model = TfIdfModel(500)

    val documents = ReutersDataset.iterator().toList
      .map(i => (i.rawFeature("Topic").asString, i.rawFeature("Text").asString))
      .filter{ case(topic, text) => topic == "Pos-coffee" || topic == "Pos-gold"}
      .map{ case(topic, text) => (topic, tokenizer.tokenize(text))}
      .groupBy(_._1)
      .map(d => (d._1, d._2.map(_._2)))
    val coffeeDocuments = documents("Pos-coffee")
    val goldDocuments = documents("Pos-gold")

    // When
    (coffeeDocuments ++ goldDocuments).foreach{ terms =>
      model = model.add(terms)
    }

    // Then
    val coffeeFeatures = coffeeDocuments.map(terms => model.tfIdf(terms))
    val goldFeatures = goldDocuments.map(terms => model.tfIdf(terms))

    val coffee2coffeeDistances = coffeeFeatures.sliding(2).map(vectors => cosineDistance(vectors(0), vectors(1))).toList
    val gold2goldDistances = goldFeatures.sliding(2).map(vectors => cosineDistance(vectors(0), vectors(1))).toList
    val coffee2goldDistances = (coffeeFeatures, goldFeatures).zipped.map{case (coffeeVector, goldVector) => cosineDistance(coffeeVector, goldVector)}.toList

    mean(coffee2coffeeDistances) should be < mean(coffee2goldDistances)
    mean(gold2goldDistances) should be < mean(coffee2goldDistances)
  }


  def mean(values: Iterable[Double]): Double = values.sum / values.size
}
