package com.github.bbiletskyy.fraud

/**
 * Created by zhengqh on 15/10/12.
 */
import akka.actor.{ Actor, ActorRef }
import com.datastax.driver.core._
import spray.http.MediaTypes.{ `application/json`, `text/html` }
import spray.httpx.SprayJsonSupport.{ sprayJsonMarshaller, sprayJsonUnmarshaller }
import spray.json.JsonParser
import spray.routing._
import scala.collection.JavaConversions._
import scala.xml.Elem

import com.github.bbiletskyy.fraud.RandomTransaction._

/** Actor Service that  is responsible of serving REST calls */
class RestServiceActor(connector: ActorRef) extends Actor with RestService {
  def actorRefFactory = context
  def receive = runRoute(route)
  def communicate(t: Transaction) = connector ! t
  override def preStart() = println(s"Starting rest-service actor at ${context.self.path}")
  val session = Cluster.builder().addContactPoint("127.0.0.1").build().connect("fraud")
  def clean() = session.execute("TRUNCATE fraud_transactions")
  def selectFraudHTML() = session.execute("select * from fraud_transactions").iterator().map(t => <p>{ t.getString("transaction") }</p>)
  def indexHtml(): Elem =
    <html>
      <body>
        <h1>Transaction Fraud Detection Engine REST API</h1>
        <a href="/fraud">View Detected Fraud Transactions</a>
        <br/>
        <a href="/transactions">View Random Transaction Examples</a>
      </body>
    </html>

  def cleanHtml(): Elem = {
    clean()
    <html>
      <body>
        <h1>Fraud transactions have been cleaned</h1>
        <a href="/fraud">View Detected Fraud Transactioons</a>
      </body>
    </html>
  }
  def fraudHtml(): Elem =
    <html>
      <body>
        <h1>Detected Fraud Transactions:</h1>
        <a href="/clean">Clean</a>
        <p>{ selectFraudHTML() }</p>
      </body>
    </html>
}

/** This trait defines the routing routines. */
trait RestService extends HttpService {
  import TransactionJsonProtocol._
  def communicate(t: Transaction)
  def indexHtml(): Elem
  def cleanHtml(): Elem
  def fraudHtml(): Elem

  val route =
    path("") {
      get {
        respondWithMediaType(`text/html`) {
          complete {
            indexHtml()
          }
        }
      }
    } ~ path("transaction") {
      post {
        entity(as[Transaction]) { transaction =>
          complete {
            communicate(transaction)
            transaction
          }
        }
      }
    } ~ path("transactions") {
      get {
        respondWithMediaType(`application/json`) {
          complete { randomTransactions(10) }
        }
      }
    } ~ path("fraud") {
      get {
        respondWithMediaType(`text/html`) {
          complete {
            fraudHtml()
          }
        }
      }
    } ~ path("clean") {
      get {
        respondWithMediaType(`text/html`) {
          complete {
            cleanHtml()
          }
        }
      }
    }
}
