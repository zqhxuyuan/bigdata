package com.td.bigdata.spark.intro

import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AmpCamp5 {

  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    //pagecounts
    //tachyon
    sparkSQL
  }

  // ----------------------------------------------------------------------------
  // http://ampcamp.berkeley.edu/5/exercises/introduction-to-the-scala-shell.html
  // ----------------------------------------------------------------------------
  def introScala {
    // 2. Declare a list of integers as a variable
    val myNumbers = List(1, 2, 5, 4, 7, 3)

    // 3. Declare a function, cube, that computes the cube (third power) of an Int.
    def cube(a: Int): Int = a * a * a

    // 4. Apply the function to myNumbers using the map function.
    // x是List中的每个元素,对每个x都将参数传给cube,最后每个新计算出来的结果作为新的List的每个返回值.
    myNumbers.map(x => cube(x))

    println(myNumbers) // myNumber没有发生变化! List(1, 2, 5, 4, 7, 3)

    // 5. writing the function inline in a map call, using closure notation.
    println(myNumbers.map{x => x * x * x})  // 发生了变化! List(1, 8, 125, 64, 343, 27)

    // 6. Define a factorial function that computes n! = 1 * 2 * … * n given input n
    // Then compute the sum of factorials in myNumbers.
    def factorial(n:Int):Int = if (n==0) 1 else n * factorial(n-1)

    // 函数作为参数,myNumbers的每个元素作为map函数里的factorial函数的参数,由于只有一个参数,可以省略
    myNumbers.map(factorial).sum
    myNumbers.map(factorial _).sum
    myNumbers.map(x => factorial(x)).sum

    // 7. Do a wordcount of a textfile.
    // create and populate a Map with words as keys and counts of the number of occurrences of the word as values.
    import collection.mutable.HashMap
    import scala.io.Source

    // load a text file as an array of lines
    // lines是一个数组:Array[String],数组的每个元素是文件的每一行
    val lines = Source.fromFile("/home/hadoop/data/README.md").getLines.toArray
    // scala访问数组的方式是(),而不是[]
    println(lines(0))

    // instantiate a collection.mutable.HashMap[String,Int] and use functional methods to populate it with wordcounts.
    // counts是一个HashMap, 其中Map[k,v]的默认v值为0. 即当第一次出现key时,默认它对应的value=0
    val counts = new HashMap[String, Int].withDefaultValue(0)
    println(counts("no-key"))  // It's 0

    // flatMap, 先flat,再Map? 还是先map,再flat? Offcourse first flat, then map!
    // lines=Array[String],其中数组的元素是每一行的内容 ==> 对应了下面的line参数
    // 经过flat后会将所有的行组成一个字符串. 即整个文件的内容都包含在一个字符串里面
    // flatMap接受一个map参数:字符串的split会返回一个字符串数组:String[] arr = String.split(" ")
    // 所以最后的返回值是文件的每一个单词:Array[String],其中数组的每个元素是每个单词
    // 上一步的Array[String]是单词数组,其中每个单词会作为foreach的参数word
    // 由于counts是一个Map,所以Map(key)=value
    lines.flatMap(line => line.split(" ")).foreach(word => counts(word) += 1)

    // just a test
    var ln=0
    lines.flatMap(line => {
      ln = ln + 1
      if(ln < 10){
        println("LINE"+ln + ":" + line)
      }
      line.split(" ")
    })

    println(counts.take(10).mkString("\n"))
    println("------------------------------")

    // Or a purely functional solution:
    val emptyCounts = Map[String,Int]().withDefaultValue(0)

    val words = lines.flatMap(line => line.split(" "))

    val counts2 = words.foldLeft(emptyCounts)({
      (currentCounts: Map[String,Int], word: String) => currentCounts.updated(word, currentCounts(word) + 1)
    })

    println(counts2.take(10).mkString("\n"))
  }

  // -------------------------------------------------------------------------
  // http://ampcamp.berkeley.edu/5/exercises/data-exploration-using-spark.html
  // -------------------------------------------------------------------------
  def pagecounts {
    // 1. Warm up by creating an RDD named pagecounts from the input files.
    val pagecounts = sc.textFile("file:/home/hadoop/data/pagecounts")

    // 2. Take a peek at the data, use the take operation of an RDD to get the first K records.
    println(pagecounts.take(10).mkString("\n"))
    pagecounts.take(10).foreach(println)
    /** 0               1     2                 3   4
      * 20090505-000000 aa.b  Wikibooks:About   1   15719
      * 20090505-000000 aa    ?14mFX1ildVnBc    1   13205
      * 20090505-000000 aa    ?53A%2FuYP3FfnKM  1   13207
      */

    // 3. how many records in total are in this data set
    pagecounts.count

    // 1.derive an RDD containing only English pages from pagecounts.
    // For each record, we can split it by the field delimiter and get the second field and then compare it with the string “en”.
    val enPages = pagecounts.filter(_.split(" ")(1) == "en").cache

    // 2. But since enPages was marked as “cached” in the previous step,
    enPages.count
    // if you run count on the same RDD again, it should return an order of magnitude faster.
    enPages.count

    // 3. Generate a histogram of total page views on Wikipedia English pages for the date range represented in our dataset
    // First, we generate a key value pair for each line; the key is the date, and the value is the number of pageviews for that date
    // Next, we shuffle the data and group all values of the same key together. Finally we sum up the values for each key.
    val enTuples = enPages.map(line => line.split(" "))
    val enKeyValuePairs = enTuples.map(line => (line(0).substring(0, 8), line(3).toInt))
    enKeyValuePairs.reduceByKey(_+_, 1).collect

    // all-in-one together
    enPages.map(line => line.split(" ")).map(line => (line(0).substring(0, 8), line(3).toInt)).reduceByKey(_+_, 1).collect

    // 4. find pages that were viewed more than 200,000 times
    enPages.map(l => l.split(" "))          // split each line of data into its respective fields.
      .map(l => (l(2), l(3).toInt))   // extract the fields for page name and number of page views.
      .reduceByKey(_+_, 40)           // reduce by key again, this time with 40 reducers.
      .filter(x => x._2 > 200000)     // filter out pages with less than 200,000 total views
      .map(x => (x._2, x._1))         // change position of pagename and #pageviews
      .collect.foreach(println)       // print the result
  }

  // ----------------------------------------------------
  // http://ampcamp.berkeley.edu/5/exercises/tachyon.html
  // ----------------------------------------------------
  def tachyon{
    // Because /LICENSE is in memory, when a new Spark program comes up, it can load in memory data directly from Tachyon
    sc.hadoopConfiguration.set("fs.tachyon.impl", "tachyon.hadoop.TFS")

    // Input/Output with Tachyon
    var file = sc.textFile("tachyon://localhost:19998/LICENSE")
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    counts.saveAsTextFile("tachyon://localhost:19998/result")

    // Store RDD OFF_HEAP in Tachyon
    counts.persist(org.apache.spark.storage.StorageLevel.OFF_HEAP)
    counts.take(10)
    counts.take(10)
  }

  // -----------------------------------------------------------------------------
  // http://ampcamp.berkeley.edu/5/exercises/data-exploration-using-spark-sql.html
  // -----------------------------------------------------------------------------
  def sparkSQL {


  }


  // --------------------------------------------------------------------
  // http://spark.apache.org/docs/latest/streaming-programming-guide.html
  // --------------------------------------------------------------------
  def streamming {
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val ssc = SparkUtil.getStreamContext("local[2]", "NetworkWordCount")

  }

  // ------------------------------------------------------------------------
  // http://ampcamp.berkeley.edu/5/exercises/graph-analytics-with-graphx.html
  // ------------------------------------------------------------------------
  def graphX {
    //2.1. The Property Graph
    //User: userId,(userName,age)
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //Link: fromUser, toUser, weight
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    //From Array to Make a RDD
    //顶点RDD, 顶点的值=Long, 顶点的属性是一个元组String的name和Int的age
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    //边RDD: 边的属性是Int类型, 表示权重. Edge中封装了源,目标,权重
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //Make Graph with vertex and edge
    //图的表示使用顶点RDD和边RDD. 但是Graph的类型没有顶点的值和边的值
    //而是顶点的属性和边的属性. 因为顶点的值只能是Long类型的!
    //边由于有出边和入边, 而它们对应的分别是源顶点和目标顶点. 所以Edge的前两个参数也固定是Long类型的
    //所以构造图的时候不需要知道顶点的值的类型. 只需要知道顶点的属性和边的属性. 这两个是可以自定义的
    //Graph的泛型为: Graph[VD, ED]. 其中VD:VertexData, ED:EdgeData
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    //2.2. Graph Views
    //----------------------------------------- Graph Views
    /**
     *  David is 42
     *  Fran is 50
     *  Charlie is 65
     *  Ed is 55
     */
    // Solution 1
    graph.vertices.filter {
      case (id, (name, age)) => age > 30
    }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }

    // Solution 2
    graph.vertices.filter(v => v._2._2 > 30).collect.foreach(
      v => println(s"${v._2._1} is ${v._2._2}")
    )

    // Solution 3
    for ((id,(name,age)) <- graph.vertices.filter {
      case (id,(name,age)) => age > 30
    }.collect) {
      println(s"$name is $age")
    }

    //----------------------------------------- display who likes who
    for (triplet <- graph.triplets.collect) {
      /**
       * Triplet has the following Fields:
       * triplet.srcAttr: (String, Int) // triplet.srcAttr._1 is the name
       * triplet.dstAttr: (String, Int)
       * triplet.attr: Int
       * triplet.srcId: VertexId
       * triplet.dstId: VertexId
       */
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }

    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} loves ${triplet.dstAttr._1}")
    }

    // 3. Graph Operators
    //图的入度
    //VertexRDD[Int] = RDD[(VertexId, Int)]
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    inDegrees.collect.foreach(println _)
    /**
      (4,1)     4有一个节点指向它
      (1,2)     1有2个节点指向它
      (6,2)     6有2个节点指向它
      (2,2)     ...
      (3,1)
     */
    val outDegrees: VertexRDD[Int] = graph.outDegrees
    outDegrees.collect.foreach(println _)
    /**
      (4,1)     4往外指出了一个节点
      (5,3)     5往外指出了3个节点
      (2,2)     ...
      (3,2)
    */

    // Define a class to more clearly model the user property
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

    // Create a user Graph
    //def mapVertices[VD2](map: (VertexID, VD) => VD2)
    // : Graph[VD2, ED]
    //Graph[VD,ED] --mapVertices--Graph[VD2,ED]
    //mapVertices的类型是VD2, 即map的返回值是VD2, here VD2=User
    //map函数的参数是VertexID,VD, 其中VertexID=id, VD=(name,age)
    val initialUserGraph: Graph[User, Int] = graph.mapVertices{
      case (id, (name, age)) => User(name, age, 0, 0)
    }

    // Fill in the degree information
    // def outerJoinVertices[U, VD2](other: RDD[(VertexID, U)])
    //   (mapFunc: (VertexID, VD, Option[U]) => VD2)
    //   : Graph[VD2, ED]
    //outerJoinVertices方法的参数是other: RDD[(VertexID, U)],
    //   这里是initialUserGraph.inDegrees = inDegrees: VertexRDD[Int] = RDD[(VertexId, Int)], 所以U=Int
    //case中的是mapFunc, 参数(VertexID, VD, Option[U]) = (id, u, inDegOpt), 所以
    //   VertexID=id, VD=User u,
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    }
  }
}