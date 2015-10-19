package com.chinahadoop.scala

/**
 * Created by chenchao on 14-2-23.
 */
class Basic6 {

}

class A{

}

class RichA(a : A){
  def rich{
    println("rich...")
  }
}


object Basic6 extends App{

  implicit def a2RichA(a : A) = new RichA(a)

  val a = new A
  a.rich

  def testParam(implicit name : String){
    println(name)
  }

  implicit val name = "implicit!!!"

  testParam
  testParam("xx")

  implicit class Calculator(x : Int){
    def add(a : Int)  : Int = a + 1
  }

  //import xx.xx.Calculator
  println(1.add(1))
}
