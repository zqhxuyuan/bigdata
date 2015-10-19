package com.td.bigdata.es.intro

import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.elasticsearch.hadoop.mr.EsInputFormat

import org.elasticsearch.spark.sql._

import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

/**
 * Created by zhengqh on 15/6/25.
 */
object Tutorial {

  val conf = new SparkConf().setAppName("ESHadoopDemo").setMaster("local")
  conf.set("spark.es.index.auto.create", "true")
  conf.set("spark.es.nodes","localhost:9200")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // this is used to implicitly convert an RDD to a DataFrame. 非常重要,否则toDF会报错.
  import sqlContext.implicits._

  // case class used to define the DataFrame
  // 注意: 要把case class放外边,否则报错:
  //   Error:(35, 11) No TypeTag available for Person
  //                    .map(p => Person(p(0), p(1), p(2).trim.toInt))
  // 见SO: http://stackoverflow.com/questions/29143756/scala-spark-app-with-no-typetag-available-error-in-def-main-style-app
  case class Person(name: String, surname: String, age: Int)

  // ES-Spark Options
  val options = Map(
    "pushdown" -> "true",
    "es.nodes" -> "localhost",
    "es.port" -> "9200")

  // ===== TEST CASE FROM https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html =====

  //------------------------------------------------------------------------写⬇
  def insertMap2ES(): Unit ={
    // write data to ES, so data must be map type as ES accept JSON document
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    //rdd可以直接save到ES中
    //1.
    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

    //2.
    val rdd = sc.makeRDD(Seq(numbers, airports))
    rdd.saveToEs("spark/docs")

    //3.通过EsSpark的save方法,不过要传入你的rdd
    EsSpark.saveToEs(rdd, "spark/docs")

    //下面我们优先选择第一种1., 除非不满足的情况采用3.
  }

  def insertCaseClass2ES(): Unit ={
    // define a case class
    case class Trip(departure: String, arrival: String)
    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    sc.makeRDD(Seq(upcomingTrip, lastWeekTrip)).saveToEs("spark/docs")
  }


  def insertJSON2ES(): Unit ={
    // Writing existing JSON to Elasticsearch
    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""

    sc.makeRDD(Seq(json1, json2)).saveJsonToEs("spark/json-trips")
  }


  def insertDynamicTable(): Unit ={
    // Writing to dynamic/multi-resources
    val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

    sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection/{media_type}")
  }


  def insertWithMeta2ES(): Unit ={
    // Handling document metadata
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
    EsSpark.saveToEsWithMeta(airportsRDD, "airports/2015")

    // metadata for each document
    // note it's not required for them to have the same structure
    val otpMeta = Map("ID" -> 1, "TTL" -> "3h")
    val mucMeta = Map("ID" -> 2, "VERSION" -> "23")
    val sfoMeta = Map("ID" -> 3)

    val airportsRDD2 = sc.makeRDD(Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo)))
    EsSpark.saveToEsWithMeta(airportsRDD2, "airports/2015")
  }

  def df2ES(): Unit ={
    //  create DataFrame
    val people = sc.textFile("/Users/zhengqh/data/people.txt")
      .map(_.split(","))
      .map(p => Person(p(0), p(1), p(2).trim.toInt))
      .toDF()

    people.saveToEs("spark/people")
    //http://localhost:9200/spark/people/_search可以查看spark/people这张表下的所有记录
  }

  //-------------------------------------------------------------------------------读⬇

  def readFromES(): Unit ={
    // Reading data from Elasticsearch
    //q=xxx, 其中xxx表示值,而不是key!
    val rdd = sc.esRDD("spark/json-trips", "?q=*O*")
    println(rdd)
    println(rdd.count)
    println(rdd.first)  //(AU4p9pIW-swhL1EH6bdc,Map(arrival -> Otopeni, SFO -> San Fran))
    //rdd中每条记录的结构是: PairRDD<String, Map<String, Object>>
    //a Tuple2 containing as the first element the document id and the second element
    //the actual document represented through Scala collections, namely one `Map[String, Any]`
    //where the keys represent the field names and the value their respective values.
    println(rdd.toArray().foreach(println))
  }

  def es2Map(): Unit = {
    val rdd = sc.esRDD("spark/json-trips", "?q=*O*")

    //只获取Map中指定的key,比如json-trips中key=airport
    //row的结构是DocumentId, Map<Key,Value>. 其中DocumentID是ES中生成的唯一ID
    //Map结构才是我们关注的,因为我们放入的时候就是把Map放进去的.所以取得row._2表示得到了这个Map
    //得到map后就很简单了,map.get(key)就能获取到指定key的value,表示我们只关注map的这个key!
    rdd.collect().foreach(row => row._2.get("airport").foreach(println))
  }


  def es2DF_Load(): Unit ={
    // Spark 1.4 style
    val df = sqlContext.read.format("org.elasticsearch.spark.sql")
      .options(options)
      .load("spark/people")  //读取spark数据库(index)的people表(type)

    //下面的操作就是DF的操作了
    df.show()
    df.printSchema
    //push down to es server for dsl query, only return need results
    val filter = df.filter(df("name").equalTo("Andy").and(df("age").gt(20)))
    filter.show
  }

  //读取成DF的另一种方式,可以添加查询条件, 类似esRDD的第二个参数query json.
  def es2DF(): Unit ={
    //Spark SQL
    //CREATE TEMPORARY TABLE people USING org.elasticsearch.spark.sql OPTIONS (path "spark/people")
    //SELECT * FROM people WHERE name = "Andy" and age > 3

    /*
    sqlContext.sql(
      "CREATE TEMPORARY TABLE tmpPeople    " +
        "USING org.elasticsearch.spark.sql " +
        "OPTIONS ( resource 'spark/people', nodes 'localhost', port '9200')")
    */

    val peopleDF = sqlContext.esDF("spark/people")
    // check the associated schema, the same as df.printSchema above
    println(peopleDF.schema.treeString)

    // get only the Smiths.
    val smiths = sqlContext.esDF("spark/people","?q=Feng" )
    smiths.show()
  }

  import org.apache.hadoop.io.{MapWritable, Text, NullWritable}
  import org.elasticsearch.hadoop.mr.EsOutputFormat

  @deprecated
  def hadoopRdd(): Unit ={
    //设置项不再是Map类型,而是Hadoop专有的JobConf. 或者采用下面的sc.hadoopConfiguration.set
    val conf = new JobConf()
    conf.set("es.resource", "radio/artists")
    conf.set("es.query", "?q=me*")

    //读取出来的记录为key-value形式，key为Text类型，value为MapWritable类型
    val esRDD = sc.hadoopRDD(conf,
      classOf[EsInputFormat[Text, MapWritable]],  //Input
      classOf[Text],              //Key: Text
      classOf[MapWritable])       //Value: MapWritable. 对应了esRDD的RDD[String,Map[String,Any]]中的Map.

    val docCount = esRDD.count();
  }

  def systemLog(): Unit ={
    var systemlog = sc.textFile("/var/log/system.log")
    val Pattern = """(\w{3}\s+\d{1,2} \d{2}:\d{2}:\d{2}) (\S+) (\S+)\[(\d+)\]: (.+)""".r

    //RDD[Map[(String,String)]]
    var entries = systemlog.map {
      case Pattern(timestamp, host, program, pid, message)
        => Map("timestamp" -> timestamp, "host" -> host, "program" -> program, "pid" -> pid, "message" -> message)
      case (line) => Map("message" -> line)
    }
    entries.saveToEs("spark/docs")
  }

  /**
  {
      "_index": "syslog",
      "_type": "entry",
      "_id": "ThOMFd8TSViYMtEOeq6eBw",
      "_score": 1,
      "_source": {
         "hostname": "oolong",
         "process": "pulseaudio",
         "timestamp": "Nov 12 14:34:15",
         "message": "[alsa-sink] alsa-sink.c: We were woken up with POLLOUT set -- however a subsequent snd_pcm_avail() returned 0 or another value < min_avail.",
         "pid": "2139"
      }
   }
   */
  def syslog(): Unit ={
    val syslog = sc.textFile("/var/log/syslog")
    val re = """(\w{3}\s+\d{2} \d{2}:\d{2}:\d{2}) (\w+) (\S+)\[(\d+)\]: (.+)""".r
    val entries = syslog.collect {
      case re(timestamp, hostname, process, pid, message) =>
        Map("timestamp" -> timestamp, "hostname" -> hostname, "process" -> process, "pid" -> pid, "message" -> message)
    }
    // convert to Writables: RDD[MapWritable]
    val writables = entries.map(toWritable)
    // message the types so the collection is typed to an OutputFormat[Object, Object]
    val output = writables.map { v => (NullWritable.get.asInstanceOf[Object], v.asInstanceOf[Object]) }
    // index the data to elasticsearch
    sc.hadoopConfiguration.set("es.resource", "syslog/entry")
    //output.saveAsHadoopFile[EsOutputFormat]("-")

    //sample data
    val tweet = Map("user" -> "kimchy", "post_date" -> "2009-11-15T14:12:12", "message" -> "trying out Elastic Search")
    val tweets = sc.makeRDD(Seq(tweet))
    val samWritables = tweets.map(toWritable)
    val samOutput = samWritables.map { v => (NullWritable.get : Object, v : Object) }  //没有使用asInstanceOf,直接用:声明为Object
    samOutput.saveAsHadoopFile("")
  }

  // helper function to convert Map to a Writable
  def toWritable(in: Map[String, String]) = {
    val m = new MapWritable
    for ((k, v) <- in)
      m.put(new Text(k), new Text(v))
    m
  }

  def main(args: Array[String]): Unit = {
    /*
    insertCaseClass2ES();
    insertDynamicTable();
    insertJSON2ES();
    insertMap2ES();
    insertWithMeta2ES();
    */

    systemLog()
  }

}
