package sparkly.web.resource

import org.json4s.{DefaultFormats, Formats}
import org.scalatra.ScalatraServlet
import org.scalatra.json.JacksonJsonSupport
import org.slf4j.LoggerFactory

class BaseResource extends ScalatraServlet with JacksonJsonSupport {

  val logger = LoggerFactory.getLogger(this.getClass)

  protected implicit val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

  error {
    case e: IllegalArgumentException => {
      logger.error("Illegal argument error", e)
      throw e
    }
    case e: Throwable => {
      logger.error("Unexpected error", e)
      throw e
    }
  }
}
