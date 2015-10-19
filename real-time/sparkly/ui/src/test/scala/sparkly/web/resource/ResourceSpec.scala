package sparkly.web.resource

import org.scalatest._
import org.scalatra.test.scalatest._
import sparkly.config.Bindings

trait ResourceSpec extends ScalatraFlatSpec with Matchers with Bindings {

  /*
  override def baseUrl: String =
    server.getConnectors.headOption match {
      case Some(conn: Connector) =>
        val host = Option(conn.getHost) getOrElse "localhost"
        val port = conn.getLocalPort
        require(port > 0, "The detected local port is < 1, that's not allowed")

        "http://%s:%d".format(host, port)
      case None =>
        sys.error("can't calculate base URL: no connector")
    }
    */
}