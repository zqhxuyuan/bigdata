package com.chinahadoop.scala

/**
 * Created by chenchao on 14-2-22.
 */
object Basic{

  def hello(name : String = "ChinaSpark") : String = { //public static String hello(String name)={ return xxxxx }
    return "Hello : " + name
  }

  def helloScala(){
    println("hello Scala!!!")
  }

  val add = (x : Int, y : Int) => x + y

  def add2(x:Int)(y:Int) = x + y

  def printEveryChar(c : String*) = {
   c.foreach(x => println(x))
  }

  def main(args : Array[String]){

    //println("Hello Scala")
    //println(hello("Scala"))
    //helloScala()
    //add(1,2)
    //println(add2(1)(2))
    //printEveryChar("a","b","c","d")
    //println(hello())
//    val x = 1
//    val a = if(x > 0) 1 else 0
//    println(a)

//    var (n,r) = (10,0)
//    while(n > 0){
//      r = r + n
//      n = n - 1
//    }
//    println(r)

    //foreach

//    for(i <- 1 until 10){  //a.b("xxx") === a b "xxx"
//      println(i)
//    }

//    for(i <- 1 to 10 if i % 2 == 0){
//      println(i)
//    }

  }
}