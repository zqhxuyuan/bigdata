import javax.servlet.ServletContext
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.LifeCycle
import org.scalatra.servlet.ScalatraListener
import sparkly.config._
import sparkly.web.resource._
import sparkly.config.SparklyConfig._

class ScalatraBootstrap extends LifeCycle with Bindings {
  override def init(context: ServletContext) {
    context.mount(new PipelineResource, "/api/pipelines/*")
    context.mount(new ComponentResource, "/api/components/*")
    context.mount(new LocalClusterResource(), "/api/clusters/local/*")
    context.mount(new PipelineValidationResource(), "/api/pipeline-validation/*")
    context.mount(new MonitoringResource(), "/api/monitoring/*")
  }
}

object Boot {
  def main(args: Array[String]) {
    val port = WEB_PORT

    val server = new Server(port)
    val context = new WebAppContext()

    context setContextPath "/"
    context.setResourceBase(WEB_SOURCES)
    context.addEventListener(new ScalatraListener)
    context.setInitParameter("org.atmosphere.websocket.maxTextMessageSize", "327280")

    server.setHandler(context)
    server.start
    server.join
  }
}
