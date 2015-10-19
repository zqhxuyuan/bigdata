package com.chinahadoop.scala

/**
 * Created by chenchao on 14-2-22.
 */

class ApplyTest{
  def apply() = "APPLY"
  def test{
    println("test")
  }
}

object ApplyTest{
  var count = 0

  def apply() = new ApplyTest

  def static{
    println("i'm a static method")
  }

  def incr = {
    count = count + 1
  }
}


class Basic4 {

}

object  Basic4 extends App{
  //ApplyTest.static
//  val a = ApplyTest()
//  a.test

//  val t = new ApplyTest
//  println(t())
//  println(t)

  for(i <- 1 to 10 ){
    ApplyTest.incr
  }
  println(ApplyTest.count)

}

/*    Play
*
*      package com.chinahadoop{
*       //----------------
*       package spark{
*       }
*      }
*
*     package com.a
*     package b
*
*
*      {
*        import xx.xxx.xyy
*      }
*
*      import java.util.{HashMap => JavaHashMap}
*
*      HashMap => _
*
*
*      package aa.bb.cc.dd
*
*      class XXX{
*         private[dd] def asasas = {}
*      }
*
* */