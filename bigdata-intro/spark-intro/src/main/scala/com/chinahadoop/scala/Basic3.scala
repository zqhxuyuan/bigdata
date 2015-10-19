package com.chinahadoop.scala

/**
 * Created by chenchao on 14-2-22.
 */
class Basic3 {

}

abstract class Person1{
  def speak
  val name : String
  var age : Int
}

class Student1 extends Person1{
  def speak {
    println("speak!!!")
  }

  val name = "AAA"
  var age = 100
}

//trait Logger{
//  def log(msg : String) {
//    println("log " + msg)
//  }
//}
//
//class Test extends Logger{
//  def test{
//    log("xxx")
//  }
//}

//trait Logger{
//  def log(msg : String)
//}
//
//trait ConsoleLogger extends Logger{
//  def log(msg : String) {
//    println(msg)
//  }
//}
//
//class Test extends ConsoleLogger{
//  def test{
//    log("PPP")
//  }
//}


trait ConsoleLogger{
  def log(msg : String){
    println("save money : " + msg)
  }
}

trait MessageLogger  extends ConsoleLogger{
   override def log(msg : String){
    println("save money to bank : " + msg)
  }
}

abstract class Account{
  def save
}

class MyAccount extends Account with ConsoleLogger{
  def save{
    log("100")
  }
}

object Basic3 extends App{

//  val acc = new MyAccount
//  acc.save

  val acc = new MyAccount with MessageLogger
  acc.save

//  val t = new Test
//  t.test

//  val s = new Student1
//  s.speak
//
//  println(s.name + " : " +s.age )

  //trait

}
