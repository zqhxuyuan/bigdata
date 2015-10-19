package cn.chinahadoop.ml

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by chenchao on 14-3-24.
 */
object LinearRegression {
  def main(args : Array[String]){
    val sc = new SparkContext("local[2]","BinaryClassification","/Users/chenchao/software/spark-0.9.0-incubating-bin-hadoop1")

    val data = sc.textFile("/Users/chenchao/workspace/chinahadoop/data/ridge-data/lpsa.data")

    val parsedData = data.map { line =>
      val parts = line.split(',')
      //LabeledPoint(parts(0).toDouble, parts(1).split(' ').map(x => x.toDouble).toArray)
      LabeledPoint(parts(0).toDouble, parts(1).split(' ').toVector)
    }

    //构建模型
    val numIterations = 20
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)

    //预测
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //计算MSE
    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2)}.reduce(_ + _)/valuesAndPreds.count
    println("training Mean Squared Error = " + MSE)
  }
}
