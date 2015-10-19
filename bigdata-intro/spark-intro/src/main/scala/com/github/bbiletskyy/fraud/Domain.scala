package com.github.bbiletskyy.fraud

/**
 * Created by zhengqh on 15/10/12.
 */
import org.apache.spark.mllib.linalg.Vectors
import spray.json.DefaultJsonProtocol
import java.util.UUID._

case class Transaction(id: String, user: String, receiver: String, amount: String, timestamp: String)

object TransactionJsonProtocol extends DefaultJsonProtocol {
  implicit val TransactionFormat = jsonFormat5(Transaction)
}

object Domain {
  val receivers = Seq("Albert Hein", "E-Bay", "Power Company")
  val receiverIds = Map(receivers(0) -> 0, receivers(1) -> 1, receivers(2) -> 2)

  def features(t: Transaction) = Vectors.dense(receiverId(t), amountId(t))

  def receiverId(t: Transaction): Int = receiverIds(t.receiver)

  def amountId(t: Transaction): Int = {
    val amount = t.amount.toDouble
    if (amount < 0.0) throw new IllegalArgumentException(s"Amount can not be negative, amount = $amount")
    if (amount < 1.00) 0
    else if (amount < 100) 1
    else 2
  }
}

object RandomTransaction {
  val rnd = new scala.util.Random()
  def randomTransaction() = apply()
  def randomFraud() = Transaction(randomUUID.toString, randomUUID.toString, Domain.receivers(1), generateRandomAmount(0,0).toString, timestamp)
  def apply(): Transaction = Transaction(randomUUID.toString, randomUUID.toString, randomReceiver, randomAmount, timestamp)
  def randomReceiver() = Domain.receiverIds.keys.toSeq(rnd.nextInt(Domain.receiverIds.keys.size))
  def randomAmount() = Seq(generateRandomAmount(0,0), generateRandomAmount(10,99), generateRandomAmount(100,999))(rnd.nextInt(3)).toString()
  def timestamp() = new java.util.Date().toString()

  private def generateRandomAmount(minBound : Int = 0, maxBound: Int): Double ={
    minBound + (if(maxBound==0) 0 else rnd.nextInt(maxBound)) + (rnd.nextInt(99).toDouble /100)
  }

  def randomTransactions(size : Int) = {
    Array.fill[Transaction](size)(apply())
  }
}
