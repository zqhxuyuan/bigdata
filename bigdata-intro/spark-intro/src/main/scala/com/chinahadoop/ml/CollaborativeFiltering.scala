package com.chinahadoop.ml

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by chenchao on 14-3-24.
 */
object CollaborativeFiltering {
  def main(args : Array[String]){
    val sc = new SparkContext("local[2]","BinaryClassification","/Users/chenchao/software/spark-0.9.0-incubating-bin-hadoop1")
    val data = sc.textFile("/Users/chenchao/workspace/chinahadoop/data/als/test.data")
    //通过数据构造Rating，本质上就是一个Tuple3[Int, Int, Double]
    val ratings = data.map(_.split(',') match {
      case Array(user, item, rate) =>  Rating(user.toInt, item.toInt, rate.toDouble)
    })

    //利用ALS构建推荐模型
    val numIterations = 20
    val model = ALS.train(ratings, 1, 20, 0.01)


    //预测
    val usersProducts = ratings.map{ case Rating(user, product, rate)  => (user, product)}
    val predictions = model.predict(usersProducts).map{
      case Rating(user, product, rate) => ((user, product), rate)
    }

    val ratesAndPreds = ratings.map{
      case Rating(user, product, rate) => ((user, product), rate)
    }.join(predictions)

    //打印出原始打分及预测打分
    ratesAndPreds.map(x => {
      val user = x._1._1
      val product = x._1._2
      val rate = x._2
      println(s"user,product,rate is $user,$product,$rate" )
    }).count

    //计算MSE误差
    val MSE = ratesAndPreds.map{
      case ((user, product), (r1, r2)) =>  math.pow((r1- r2), 2)
    }.reduce(_ + _)/ratesAndPreds.count
    println("Mean Squared Error = " + MSE)
  }
}
