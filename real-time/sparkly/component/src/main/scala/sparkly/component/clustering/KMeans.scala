package sparkly.component.clustering

import sparkly.core._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.VectorUtil._
import org.apache.spark.mllib.clustering.StreamingKMeans
import scala.util.Random
import scala.collection.mutable.ArrayBuffer

class KMeans extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "KMeans", category = "Clustering",
    description =
      """
        |Clustering based on MLlib's KMeansModel with a Kmeans ++ initialization.
      """.stripMargin,
    inputs = Map (
      "Train" -> InputStreamMetadata(namedFeatures = Map("Features" -> FeatureType.VECTOR)),
      "Predict" -> InputStreamMetadata(namedFeatures = Map("Features" -> FeatureType.VECTOR))
    ),outputs = Map (
      "Predicted" -> OutputStreamMetadata(from = Some("Predict"), namedFeatures = Map("Cluster" -> FeatureType.INTEGER))
    ), properties = Map (
      "Clusters" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(2), description = "Number of clusters"),
      "Decay factor" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(1.0), description = "Decay factor")
    )
  )

  // TODO : add kmeans cost monitoring
  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val k = context.properties("Clusters").as[Int]
    val decayFactor = context.properties("Decay factor").as[Double]

    val model = new StreamingKMeansPP()
      .setK(k)
      .setDecayFactor(decayFactor)

    val train = context.dstream("Train").map(i => i.inputFeature("Features").asVector.toDenseSpark).cache()

    // Train
    model.trainOn(train)

    // Predict
    val predictions = context.dstream("Predict", "Predicted").map{ i =>
      val features = i.inputFeature("Features").asVector.toSpark
      val cluster = model.latestModel().predict(features)
      i.outputFeature("Cluster", cluster)
    }

    Map("Predicted" -> predictions)
  }

}

class StreamingKMeansPP extends StreamingKMeans {

  override def trainOn(data: DStream[Vector]) {
    data.foreachRDD { (rdd, time) => if (!rdd.isEmpty) {
        // KMeans++ init
        if (model.clusterCenters == null) {
          val data = rdd.collect()
          val centers = initCentroids(data)
          setInitialCenters(centers, Array.fill(k)(0.0))
        }

        model = model.update(rdd, decayFactor, timeUnit)
      }
    }
  }

  private def initCentroids(data: Array[Vector]): Array[Vector] = {
    val points = ArrayBuffer(data :_*)

    val centers = ArrayBuffer.empty[Vector]
    centers += points.remove(Random.nextInt(points.size))

    (1 until k).foreach{ index =>
      val dxs = points.map { point =>
        val nearestCentroid = centers.map(c => (c, Vectors.sqdist(c, point)))
        (point, nearestCentroid.maxBy(_._2)._2)
      }
      val farthest = dxs.maxBy(_._2)
      val center = dxs.find{ case (candidate, dx) => Random.nextDouble > (0.75 * Math.pow(dx / farthest._2, 2))}.getOrElse(farthest)._1
      points -= center
      centers += center
    }

    centers.toArray
  }
}