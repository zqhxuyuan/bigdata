package com.chinahadoop.scala

/**
 * Created by chenchao on 14-2-22.
 */
class Basic5 {

}

case class Book(name : String, author : String)

object Basic5 extends App{

  val value = 3

  val result = value match {
    case 1 => "one"
    case 2 => "two"
    case _ => "some other number"
  }

  val result2 = value match {
    case i if i == 1 => "one"
    case i if i == 2 => "two"
    case _ => "some other number"
  }

//  println("result of match is : " + result)
//  println("result2 of match is : " + result2)

  def t(obj : Any) = obj match {
    case x : Int => println("Int")
    case s : String => println("String")
    case _ => println("unknown type")
  }

  t(1L)

  val macTalk = Book("MacTalk","CJQ")
  macTalk match {
    case Book(name, author) => println("this is a book")
    case _ => println("unknown")
  }

  val l = List(1,2,3,4)
  val s = Set(1,2,3)
  val m = Map(1 -> 1)


}