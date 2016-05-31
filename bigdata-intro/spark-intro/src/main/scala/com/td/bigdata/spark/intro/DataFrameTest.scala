package com.td.bigdata.spark.intro

import java.sql.DriverManager

import org.apache.spark.rdd.{RDD, JdbcRDD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zhengqh on 15/8/11.
 *
 * http://spark.apache.org/docs/latest/sql-programming-guide.html
 */
object DataFrameTest {

  val conf = new SparkConf().setAppName("DataFrameTest").setMaster("local[2]")
  val sc = new SparkContext(conf)
  // sc is an existing SparkContext.
  val sqlContext = new SQLContext(sc)
  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext._
  import sqlContext.implicits._

  def main(args: Array[String]) {
    testMySQL()
  }

  // Define the schema using a case class.
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface.
  case class Person(name: String, age: Int)

  /**
   * Parquet使用:
   * 1.读取文件转化为DF, 或者直接使用sql(query)返回DF
   * 2.将上面的DF注册为一张临时表tbl
   * 3.cacheTable("tbl")临时表
   * 4.sql("select * from tbl")里面可以查询这张临时表.
   * 5.sql("select * from tbl").count: count触发Action,之后的sql查询都被缓存起来
   * 6.sql()返回是个DF, 可以使用DF的方法
   *
   */
  def testParquetReadWrite(): Unit = {
    // Create an RDD of Person objects and register it as a table.
    val people = sc.textFile("/user/qihuang.zheng/resources/people.txt").
      map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    // The inferred schema can be visualized using the printSchema() method.
    people.printSchema()
    // root
    //  |-- age: integer (nullable = true)
    //  |-- name: string (nullable = true)

    // Register this DataFrame as a table.
    people.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index:
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    // or by field name:
    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
    // Map("name" -> "Justin", "age" -> 19)

    // The RDD is implicitly converted to a DataFrame by implicits, allowing it to be stored using Parquet.
    people.saveAsParquetFile("/user/qihuang.zheng/resources/people.parquet")
    people.write.parquet("/user/qihuang.zheng/resources/people2.parquet")

    // Read in the parquet file created above.  Parquet files are self-describing so the schema is preserved.
    // The result of loading a Parquet file is also a DataFrame.
    val parquetFile = sqlContext.read.parquet("/user/qihuang.zheng/resources/people.parquet")

    // Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerTempTable("parquetFile")
    val teenagers2 = sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    teenagers2.map(t => "Name: " + t(0)).collect().foreach(println)

    // JOIN TABLE
    val jointbls = sql("SELECT people.name FROM people join parquetFile where people.name=parquetFile.name")
    jointbls.map(t => "Name: " + t(0)).collect().foreach(println)
  }

  // http://www.infoq.com/cn/articles/apache-spark-sql
  // 创建一个表示客户的自定义类
  case class Customer(customer_id: Int, name: String, city: String, state: String, zip_code: String)

  def testDFSimpleOperation(): Unit ={
    // 导入语句，可以隐式地将RDD转化成DataFrame
    import sqlContext.implicits._

    // 用数据集文本文件创建一个Customer对象的DataFrame
    val dfCustomers = sc.textFile("data/customers.txt").map(_.split(",")).map(p => Customer(p(0).trim.toInt, p(1), p(2), p(3), p(4))).toDF()

    // 将DataFrame注册为一个表
    dfCustomers.registerTempTable("customers")

    // 显示DataFrame的内容
    dfCustomers.show()

    // 打印DF模式
    dfCustomers.printSchema()

    // 选择客户名称列
    dfCustomers.select("name").show()

    // 选择客户名称和城市列
    dfCustomers.select("name", "city").show()

    // 根据id选择客户
    dfCustomers.filter(dfCustomers("customer_id").equalTo(500)).show()

    // 根据邮政编码统计客户数量
    dfCustomers.groupBy("zip_code").count().show()
  }

  // 用编程的方式指定模式
  def testDFSimpleAPI(): Unit ={
    // 创建RDD对象
    val rddCustomers = sc.textFile("data/customers.txt")

    // 用字符串编码模式
    val schemaString = "customer_id name city state zip_code"

    // 导入Spark SQL数据类型和Row
    import org.apache.spark.sql._
    import org.apache.spark.sql.types._;

    // 用模式字符串生成模式对象
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // 将RDD（rddCustomers）记录转化成Row。
    val rowRDD = rddCustomers.map(_.split(",")).map(p => Row(p(0).trim,p(1),p(2),p(3),p(4)))

    // 将模式应用于RDD对象。
    val dfCustomers = sqlContext.createDataFrame(rowRDD, schema)

    // 将DataFrame注册为表
    dfCustomers.registerTempTable("customers")

    // 用sqlContext对象提供的sql方法执行SQL语句。
    val custNames = sqlContext.sql("SELECT name FROM customers")

    // SQL查询的返回结果为DataFrame对象，支持所有通用的RDD操作。
    // 可以按照顺序访问结果行的各个列。
    custNames.map(t => "Name: " + t(0)).collect().foreach(println)

    // 用sqlContext对象提供的sql方法执行SQL语句。
    val customersByCity = sqlContext.sql("SELECT name,zip_code FROM customers ORDER BY zip_code")

    // SQL查询的返回结果为DataFrame对象，支持所有通用的RDD操作。
    // 可以按照顺序访问结果行的各个列。
    customersByCity.map(t => t(0) + "," + t(1)).collect().foreach(println)
  }

  def testJSONSchema(): Unit ={
    //[address: struct<city:string,state:string>, address2: struct<city:string,state:string>, name: string]
    val jsonStr =
      """
        |{"name":"Yin","address":{"city":"Columbus","state":"Ohio"},"address2":{"city":"Columbus","state":"Ohio"}}
      """.stripMargin
    val rdd = sqlContext.read.json(sc.parallelize(jsonStr :: Nil))
    rdd.printSchema()
    //没有显示指定Schema,则默认是StructType,想要将StructType转换为Map还不能转! 解决方式: 提前定义Schema
    //java.lang.ClassCastException: org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema cannot be cast to scala.collection.immutable.Map
    rdd.foreach(row => {
      //val addressMap = row.getAs[Map[String, String]]("address")
      //val addressMap2 = row.getMap(0)
      //val addressRow = row.getStruct(0)
      //val addressMap = addressRow.asInstanceOf[Map[String,String]]
    })

    //------------------------------------------------------------------
    //定义Schema
    import sqlContext.implicits._
    val innerStruct = StructType(List(
      StructField("address", MapType(StringType, StringType, true), true),
      StructField("address2", MapType(StringType, StringType, true), true),
      StructField("name", StringType, true)
    ))

    //即使JSON字符串中没有address2这个Map,在1.4+中没有问题
    val json2 =
      """
        |{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}
      """.stripMargin
    val rdd2 = sc.parallelize(json2 :: Nil)
    val df = sqlContext.jsonRDD(rdd2, innerStruct)
    df.printSchema()

    //表结构按照的是StructType, 可以看到是map,而不再是struct了
    /**
    root
     |-- address: map (nullable = true)
     |    |-- key: string
     |    |-- value: string (valueContainsNull = true)
     |-- address2: map (nullable = true)
     |    |-- key: string
     |    |-- value: string (valueContainsNull = true)
     |-- name: string (nullable = true)
      */

    //现在可以getAs强制转换为Map类型了
    df.map(row=>{
      val address = row.getAs[Map[String,String]]("address")
      val address2 = row.getAs[Map[String,String]]("address2")
      (address,address2)
    }).collect().foreach(row=>{
      println(row._1)
      println(row._2)
    })
  }

  /**
   * JSON转化为DataFrame有
   * 1.读取JSON文件
   * 2.读取JSON字符串
   * 都是使用sqlContext.read.json()方法
   */
  def testJSON2DF(): Unit ={
    // 1. A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files.
    val path = "examples/src/main/resources/people.json"
    val peopleDF = sqlContext.read.json(path)

    // 2. DataFrame can be created for a JSON dataset represented by an RDD[String] storing one JSON object per string.
    val anotherPeopleRDD = sc.parallelize(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeopleDF = sqlContext.read.json(anotherPeopleRDD)

    // 业务JSON数据解析: RDD[String]转会为DataFrame
    val json = s"""
                  |{
                  |  "accountEmail": "zhangsan@163.com",
                  |  "accountLogin": "zhangsanfwd",
                  |  "accountMobile": "18868875556",
                  |  "accountName": "张三",
                  |  "accountPhone": "051788662981",
                  |  "appName": "test111",
                  |}
     """.stripMargin
    val jsonRDD1 = sc.parallelize(json :: Nil)

    val jsonDF1 = sqlContext.read.json(jsonRDD1)
    jsonDF1.printSchema()
    jsonDF1.show
    //既然JSON已经转会为DataFrame了,DF也可以转换为RDD
    val personRdd = anotherPeopleDF.map(r=>Person(r.getString(0),r.getInt(1)))
    personRdd.filter(_.age>15).count

    // 3. jonsRDD方法接收RDD[String],返回DataFrame
    val stringRDD = sc.parallelize(Seq("""
      { "isActive": false,
        "balance": "$1,431.73",
        "picture": "http://placehold.it/32x32",
        "age": 35,
        "eyeColor": "blue"
      }""",
      """{
        "isActive": true,
        "balance": "$2,515.60",
        "picture": "http://placehold.it/32x32",
        "age": 34,
        "eyeColor": "blue"
      }""",
      """{
        "isActive": false,
        "balance": "$3,765.29",
        "picture": "http://placehold.it/32x32",
        "age": 26,
        "eyeColor": "blue"
      }"""))
    sqlContext.jsonRDD(stringRDD).registerTempTable("testjson")
    sql("SELECT age from testjson").collect
  }

  case class Score(name: String, score: Int)
  case class Age(name: String, age: Int)
  def testDFJoin(): Unit ={
    val scores = sc.textFile("scores.txt").map(_.split(",")).map(s => Score(s(0),s(1).trim.toInt))
    val ages = sc.textFile("ages.txt").map(_.split(",")).map(s => Age(s(0),s(1).trim.toInt))

    //SQL
    scores.toDF.registerTempTable("scores")
    ages.toDF.registerTempTable("ages")
    val joined = sqlContext.sql("""
      SELECT a.name, a.age, s.score
      FROM ages a JOIN scores s
      ON a.name = s.name""")
    joined.collect().foreach(println)

    //DF
    val scoresAliased = scores.toDF.as('s)
    val agesAliased = ages.toDF.as('a)
    val joined2 = scoresAliased.join(agesAliased, $"name" === $"name", "inner")
  }

  //Rdd Filter
  def bcRddFilter(): Unit ={
    //小表,可以广播到各个Worker节点上
    val bc = sc.broadcast(Array[String]("login3", "login4"))

    //大表数据
    val x = Array(("login1", 192), ("login2", 146), ("login3", 72))
    val rdd = sc.parallelize(x)

    //RDD Filter
    val filterRdd = rdd.filter(x=>bc.value.contains(x._1))

    filterRdd.collect().foreach(println)
    //(login3,72)
  }

  //DataFrame Filter
  def bcDFFilter(): Unit ={
    //小表,可以广播到各个Worker节点上
    val bc = sc.broadcast(Array[String]("login3", "login4"))
    //给定参数, 判断是否在bc所在的数据集合中
    val func: (String => Boolean) = (arg: String) => bc.value.contains(arg)

    //大表数据
    val x = Array(("login1", 192), ("login2", 146), ("login3", 72))
    val xdf = sqlContext.createDataFrame(x).toDF("name", "cnt")

    val sqlfunc = sqlContext.udf.register("whateverFunc", func)  // UserDefinedFunction(<function1>,BooleanType)

    //给定大表每条数据,判断这条数据是否在小表中, 如果在,则输出
    val filtered = xdf.filter(sqlfunc(xdf("name")))

    xdf.show()
    filtered.show()
  }


  case class Log(page: String, visitor: String)

  def pageVisitor(): Unit ={
    val logs = sc.textFile("pv.txt").map(_.split(",")).map(p => Log(p(0),p(1))).toDF()
    logs.registerTempTable("logs")

    //SQL
    sqlContext.sql("select page,count(distinct visitor) as visitor from logs group by page")

    //DataFrame
    import org.apache.spark.sql.functions._
    logs.select("page","visitor").groupBy('page).agg('page, countDistinct('visitor))
  }


  /**
   * Spark JDBC连接
   *
   */
  case class MyProduct(id : Int, price : Int)
  def testMySQL(): Unit ={
    //query init result
    val rdd = new JdbcRDD(sc, () => {
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
      },
      "SELECT id,price FROM a WHERE ID >= ? AND ID <= ?",
      1, 10, 2,
      r => r.getInt(2) //注意索引从1开始. 1=id,2=price
    ).cache //这里可以cache
    rdd.filter(_>50).count()
    rdd.collect().foreach(println)

    //update
    rdd.foreachPartition { it =>
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
      val update = connection.prepareStatement("UPDATE A SET price=? where price=?")
      for (price <- it) {
        update.setInt(1, price/2)
        update.setInt(2, price)
        update.executeUpdate
      }
    }

    //query updated result
    val rdd2 = new JdbcRDD(sc, () => {
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
    },
      "SELECT id,price FROM a WHERE ID >= ? AND ID <= ?",
      1, 10, 2,
      //可以使用索引编号(从1开始),或者用字段名称. 返回值可以构造case class
      r => MyProduct(r.getInt(1),r.getInt("price"))
    )
    rdd2.collect().foreach(println)
  }


  //http://stackoverflow.com/questions/28689186/facing-error-while-extending-scala-class-with-product-interface-to-overcome-limi
  class Info () extends Product with Serializable {
    var param1 : String = ""
    var param2 : String = ""
    var param3 : String = ""
    var param4 : String = ""
    var param5 : String = ""
    var param6 : String = ""
    var param7 : String = ""
    var param8 : String = ""
    var param9 : String = ""
    var param10 : String = ""
    var param11 : String = ""
    var param12 : String = ""
    var param13 : String = ""
    var param14 : String = ""
    var param15 : String = ""
    var param16 : String = ""
    var param17 : String = ""
    var param18 : String = ""
    var param19 : String = ""
    var param20 : String = ""
    var param21 : String = ""
    var param22 : String = ""
    var param23 : String = ""
    var param24 : String = ""
    var param25 : String = ""

    def canEqual(that: Any) = that.isInstanceOf[Info]
    def productArity = 25

    def productElement(n: Int) = n match {
      case 0 => param1
      case 1 => param2
      case 2 => param3
      case 3 => param4
      case 4 => param5
      case 5 => param6
      case 6 => param7
      case 7 => param8
      case 8 => param9
      case 9 => param10
      case 10 => param11
      case 11 => param12
      case 12 => param13
      case 13 => param14
      case 14 => param15
      case 15 => param16
      case 16 => param17
      case 17 => param18
      case 18 => param19
      case 19 => param20
      case 20 => param21
      case 21 => param22
      case 22 => param23
      case 23 => param24
      case 24 => param25
    }
  }

  def test22Field(): Unit ={
    val data = "a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a"
    val rdd = sc.textFile("/user/qihuang.zheng/f25.txt")
    rdd.collect().foreach(println)
    rdd.toDF().registerTempTable("test_table")
    sql("select * from test_table").show()
  }

  case class A(a : Int)
  def testCaseClass(): Unit ={
    //case class的对象实例,继承了Product. 正如下面的Info class手动继承Product
    val obj = A(1)
    obj.isInstanceOf[Product]
    obj.productArity //1,表示字段个数
    obj.productElement(0)
  }
}
