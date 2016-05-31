package com.td.bigdata.spark.call

import java.util.Calendar

/**
 * User: arnonrgo
 * Date: 12/7/14
 * Time: 10:36 AM
 */

object CrmCall{
  def apply(line: String) :CrmCall ={
    val values = line.split("/t")
    val userId: Long = values(0).toLong
    val callTime: Long = values(1).toLong
    val duration: Long = values(2).toLong
    val issue = values(3)
    val experience = values(4).toDouble
    val team  = values(5).toInt
    val rep = values(6).toInt
    val resolved = values(7).toBoolean
    val eventType: String = values(8)
    val comments: String = values(9)

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(callTime)
    val year: Int = cal.get(Calendar.YEAR)
    val month: Int = cal.get(Calendar.MONTH) + 1
    val day: Int = cal.get(Calendar.DAY_OF_MONTH)
    val hour: Int = cal.get(Calendar.HOUR_OF_DAY)
    CrmCall(userId, callTime, duration,issue,experience,team,rep,resolved,eventType, comments, year, month, day, hour)
  }
}
case class CrmCall(userId : Long,
                   callTime:Long,
                   duration : Long,
                   issue : String,
                   experience : Double,
                   team : Int,
                   rep : Int,
                   resolved : Boolean,
                   eventType: String,
                   comments: String,
                   year : Int,
                   month : Int,
                   day : Int,
                   hour : Int
                    ) extends Event {}