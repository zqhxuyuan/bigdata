package com.td.bigdata.spark.intro

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhengqh on 15/9/14.
 *
 */
object SparkSQLTest {
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext._
  import sqlContext.implicits._

  //分区数量
  def testPartitions(): Unit ={
    import sqlContext.implicits._

    val rdd1 = sc.parallelize(List((1,"a"),(1,"b"),(1,"c")), 2);
    rdd1.partitions

    rdd1.toDF("c1","c2").registerTempTable("test")
    sqlContext.cacheTable("test")

    val rdd2 = sqlContext.sql("select * from test")
    rdd2.repartition(2)

    var partitions = 0
    rdd2.foreachPartition(it=>partitions+=1)
    partitions

    val rddEmpty = sc.parallelize(List[String]())
    rddEmpty.take(1)
  }

  //collect_set sql and api
  case class User(id: String, name: String, vtm: String, url: String)
  def testCollectSet(): Unit = {
    //spark sql的collect_set功能
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    import sqlContext.implicits._
    val data1 = sc.parallelize(List(
      "id00,user1,2,http://www.hupu.com",
      "id01,user1,2,http://www.hupu.com",
      "id02,user1,3,http://www.hupu.com",
      "id03,user1,100,http://www.hupu.com",
      "id04,user2,2,http://www.hupu.com",
      "id05,user2,1,http://www.hupu.com",
      "id06,user2,50,http://www.hupu.com",
      "id07,user2,2,http://touzhu.hupu.com"
    ))
    data1.map(l => {
      val r = l.split(",")
      (r(0), r(1), r(2), r(3))
    }).toDF("id", "name", "cnt", "url").registerTempTable("user_cnt")
    //注意要进行distinct处理. 否则sql会返回所有行. 而所有行对于相同的partition key都是一样的结果
    sql("select name,collect_set(cnt) over(partition by name) from user_cnt").distinct.collect().foreach(println)
    //那么有没有办法不用distinct. 加上group by试试, 报错说cnt不在分组字段里. 如果放在group by里面,最后还是需要distinct的!
    sql("select name,collect_set(cnt) over(partition by name) from user_cnt group by name,cnt").collect().foreach(println)


    //用spark实现hive中的collect_set函数的功能
    //http://blog.csdn.net/xiao_jun_0820/article/details/44221505
    val data = sc.parallelize(List("id1,user1,2,http://www.hupu.com",
      "id1,user1,2,http://www.hupu.com",
      "id1,user1,3,http://www.hupu.com",
      "id1,user1,100,http://www.hupu.com",
      "id2,user2,2,http://www.hupu.com",
      "id2,user2,1,http://www.hupu.com",
      "id2,user2,50,http://www.hupu.com",
      "id2,user2,2,http://touzhu.hupu.com"
    ))
    val rdd_1 = data.map(line => {
      val r = line.split(",")
      User(r(0), r(1), r(2), r(3))
    })
    //key是用户id和名称, 实际是分组聚合的字段
    val rdd_2 = rdd_1.map(u => ((u.id, u.name), u))

    //a是结果, b是rdd_2的value即User对象. 这样聚合时, 每次将输入的User和结果运用该函数
    //用User的vtm值和结果进行组合,然后进行distinct, 最后返回的就是vtm的distinct值
    val seqOp = (a: List[String], b: User) => (b.vtm :: a).distinct
    //根据key进行聚合,还需要组合方法. 即结果和结果之间的拼接
    val combOp = (a: List[String], b: List[String]) => (a ::: b).distinct

    //分组聚合后的结果是个List[String]. 初始化为空列表
    //根据key进行聚合最后返回的是(key, List[String])
    //mapValues对List[String]进行操作,比如返回一个逗号分隔的字符串
    val rdd_3 = rdd_2.aggregateByKey(List[String]())(seqOp, combOp).mapValues(l => l.mkString(","))

    /**
    ((id1,user1),2,100,3)
      ((id2,user2),50,1,2)
      */
    rdd_3.collect.foreach(println)
  }

  //去重的实现方式: distinct,或者直接使用collect_set
  def testCollectSet2(): Unit ={
    val rdd1 = sc.parallelize(List((1,"a"),(1,"b"),(1,"c")));
    rdd1.toDF("c1","c2").registerTempTable("test")
    sqlContext.cacheTable("test")
    sqlContext.sql("select * from test").count
    sqlContext.sql("select collect_set(c2) from test where c1=1")

    val rdd = sqlContext.sql("select * from test")
    for(row <- rdd){
      println(row.get(0))
    }

    val rddvalues = sqlContext.sql("select distinct c2 from test")
    for(row <- rddvalues){
      println(row.get(0))
    }
  }


  //SQL中NPE因为没有执行Action导致!
  def testSqlInSqlNPLMoreEx(): Unit ={
    import sqlContext._
    import sqlContext.implicits._

    val rdd1 = sc.parallelize(List((1,"a"),(1,"b"),(1,"c")));
    rdd1.toDF("c1","c2").registerTempTable("test")
    cacheTable("test")

    val rdd2 = sc.parallelize(List(("a","A"),("a","AA"),("b","B"),("c","C")));
    rdd2.toDF("c1","c2").registerTempTable("test2")
    cacheTable("test2")

    val rdd = sql("select * from test").map(row=>{
      val k = row.getInt(0)
      val v = row.getString(1)

      val rddSet = sql(s"select collect_set(c2) from test2 where c1='$v'")
      val set = rddSet.first().getSeq[String](0).toSet
      (k,v,set)
    })
    //NullPointerException
    rdd.count()

    val rddX = sql("select * from test").collect().map(row=>{
      val k = row.getInt(0)
      val v = row.getString(1)

      val rddSet = sql(s"select collect_set(c2) from test2 where c1='$v'")
      //val set = rddSet.first.getSeq[String](0).toSet
      val set = rddSet.collect()(0).getSeq[String](0).toSet
      (k,v,set)
    })
    rddX
  }


  //https://forums.databricks.com/questions/956/how-do-i-group-my-dataset-by-a-key-or-combination.html
  //http://stackoverflow.com/questions/31394168/implement-sum-aggregator-on-custom-case-class-on-spark-sql-dataframe-udaf
  case class Vec(var a: Int, var b: Int) {
    def +(v: Vec): Vec = {
      a += v.a
      b += v.b
      this
    }
  }
  def testGroupBy(): Unit = {
    import org.apache.spark.sql.GroupedData
    import org.apache.spark.sql.functions._

    val sessionsDF = Seq(
      ("day1", "user1", "session1", 100.0),
      ("day1", "user1", "session2", 200.0),
      ("day2", "user1", "session3", 300.0),
      ("day2", "user1", "session4", 400.0),
      ("day2", "user1", "session4", 99.0)).toDF("day", "userId", "sessionId", "purchaseTotal")

    //DataFrame API方式
    val groupedSessions: GroupedData = sessionsDF.groupBy("day", "userId")
    val groupedSessionsDF = groupedSessions.agg($"day", $"userId", countDistinct("sessionId"), sum("purchaseTotal"))
    groupedSessionsDF.show
    /**
    +----+------+----+------+-------------------------+------------------+
    | day|userId| day|userId|COUNT(DISTINCT sessionId)|SUM(purchaseTotal)|
    +----+------+----+------+-------------------------+------------------+
    |day2| user1|day2| user1|                        2|             799.0|
    |day1| user1|day1| user1|                        2|             300.0|
    +----+------+----+------+-------------------------+------------------+
      */

    //SQL方式
    sessionsDF.registerTempTable("test_df_group_by")
    sql("SELECT day, userId, count(sessionId) as session_count, sum(purchaseTotal) as purchase_total FROM test_df_group_by GROUP BY day, userId").show()
    /**
    +----+------+-------------+--------------+
    | day|userId|session_count|purchase_total|
    +----+------+-------------+--------------+
    |day2| user1|            3|         799.0|
    |day1| user1|            2|         300.0|
    +----+------+-------------+--------------+
      */

    val a : Vec = new Vec(2,5)
    val b : Vec = new Vec(3,8)
    val rdd = sc.parallelize(List((1,a),(2,b)))
    val d = rdd.reduceByKey(_+_)

    val df = sc.parallelize(List((1,new Vec(1,2)),(2,new Vec(3,4)))).toDF
    //java.lang.ClassCastException: org.apache.spark.sql.types.IntegerType$ cannot be cast to org.apache.spark.sql.types.StructType
    df.groupBy(df("theInt")).agg(sum(df("vec"))).show

    df.printSchema()
    /**
     * root
       |-- _1: integer (nullable = false)
       |-- _2: struct (nullable = true)
       |    |-- a: integer (nullable = false)
       |    |-- b: integer (nullable = false)
     */
    df.groupBy(df("_1")).agg(sum(df("_2"))).show
    df.groupBy(df("_1")).agg(sum(df("_2.a"))).show  //这样可以,但是他不是调用case class的+方法,而是int字段的求和
    df.groupBy("_1").agg(sum("_2.a")).show //OK too
  }

  //http://stackoverflow.com/questions/25031129/creating-user-defined-function-in-spark-sql
  def testCreateUDF(): Unit ={
    val demoRdd = Seq(
      ("one,2014-06-01"),
      ("two,2014-07-01"),
      ("three,2014-08-01"),
      ("four,2014-08-15"),
      ("five,2014-09-15")
    )
    def myDateFilter(date: String) = date contains "-08-"
    val entries = sc.parallelize(demoRdd).map(_.split(",")).map(e => (e(0),e(1)))
    entries.toDF("name","dat").registerTempTable("entries")
    sqlContext.udf.register("myDateFilter", myDateFilter _)
    sql("SELECT * FROM entries WHERE myDateFilter(dat)").show()

    val demoRdd2 = Seq(
      ("one","2014-06-01"),
      ("two","2014-07-01"),
      ("three","2014-08-01"),
      ("four","2014-08-15"),
      ("five","2014-09-15")
    )
    sc.parallelize(demoRdd2).toDF("name","dat").registerTempTable("test_table_udf")
    def len(str : String) = str.length
    udf.register("len", len _)
    sql("select name,len(name) from test_table_udf").show()
    sql("SELECT * FROM test_table_udf WHERE myDateFilter(dat)").show()
  }

  //https://github.com/spirom/LearningSpark/blob/master/src/main/scala/sql/UDF.scala
  // a case class for our sample table
  case class Cust(id: Integer, name: String, sales: Double, discounts: Double, state: String)
  // an extra case class to show how UDFs can generate structure
  case class SalesDisc(sales: Double, discounts: Double)
  def testUDF_LearnSpark(): Unit ={
    // create an RDD with some data
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerTable = sc.parallelize(custs, 4).toDF()
    // DSL usage -- query using a UDF but without SQL
    def westernState(state: String) = Seq("CA", "OR", "WA", "AK").contains(state)
    // for SQL usage  we need to register the table
    customerTable.registerTempTable("customerTable")

    // 1.WHERE clause
    sqlContext.udf.register("westernState", westernState _)
    println("UDF in a WHERE")
    sqlContext.sql("SELECT * FROM customerTable WHERE westernState(state)").show

    // 2.HAVING clause
    def manyCustomers(cnt: Long) = cnt > 2
    sqlContext.udf.register("manyCustomers", manyCustomers _)
    println("UDF in a HAVING")
    sqlContext.sql(
      s"""
         |SELECT state, COUNT(id) AS custCount
         |FROM customerTable
         |GROUP BY state
         |HAVING manyCustomers(custCount)
       """.stripMargin).show

    // 3.GROUP BY clause
    def stateRegion(state:String) = state match {
      case "CA" | "AK" | "OR" | "WA" => "West"
      case "ME" | "NH" | "MA" | "RI" | "CT" | "VT" => "NorthEast"
      case "AZ" | "NM" | "CO" | "UT" => "SouthWest"
    }
    sqlContext.udf.register("stateRegion", stateRegion _)
    println("UDF in a GROUP BY")
    // note the grouping column repeated since it doesn't have an alias
    sqlContext.sql(
      s"""
         |SELECT SUM(sales) AS totalSales, stateRegion(state) AS STATE
         |FROM customerTable
         |GROUP BY stateRegion(state)
      """.stripMargin).show

    // 4.we can also apply a UDF to the result columns
    def discountRatio(sales: Double, discounts: Double) = discounts/sales
    sqlContext.udf.register("discountRatio", discountRatio _)
    println("UDF in a result")
    sqlContext.sql("SELECT id, discountRatio(sales, discounts) AS ratio FROM customerTable").show

    // 5.we can make the UDF create nested structure in the results
    def makeStruct(sales: Double, disc:Double) = SalesDisc(sales, disc)
    sqlContext.udf.register("makeStruct", makeStruct _)

    // these failed in Spark 1.3.0 -- reported SPARK-6054 -- but work again in 1.3.1
    println("UDF creating structured result")
    sqlContext.sql("SELECT makeStruct(sales, discounts) AS sd FROM customerTable").show

    println("UDF with nested query creating structured result")
    sql("SELECT id, sd.sales FROM (SELECT id, makeStruct(sales, discounts) AS sd FROM customerTable) AS d").show
  }
}
