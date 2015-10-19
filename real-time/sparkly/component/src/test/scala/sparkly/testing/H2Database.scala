package sparkly.testing

import org.h2.tools.Server
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers, FlatSpec}
import java.sql.DriverManager
import org.joda.time.DateTime

class H2Database(val port: Int = 9081, val password: String = "sa") {

  val connectUrl = s"jdbc:h2:mem:test"
  val server = Server.createTcpServer("-tcpPort", port.toString, "-tcpAllowOthers", "-tcpPassword", password)

  def start() {
    server.start()
  }

  def stop() {
    server.shutdown()
    server.stop()
  }

}

class H2DatabaseSpec extends FlatSpec with H2Embedded with Matchers {

  "H2 Database" should "respond to queries" in {
    // Given
    Class.forName("org.h2.Driver")
    val connection = DriverManager.getConnection(db.connectUrl, "sa", "sa")
    connection.setAutoCommit(true)

    val statement = connection.createStatement()
    statement.execute("DROP TABLE IF EXISTS USER")
    statement.execute("CREATE TABLE USER(ID INT PRIMARY KEY, NAME VARCHAR, CREATION BIGINT)")

    // When
    statement.execute(s"INSERT INTO USER (ID, NAME, CREATION) VALUES (1, 'Julie', ${DateTime.parse("1981-10-16").getMillis})")
    statement.execute(s"INSERT INTO USER (ID, NAME, CREATION) VALUES (2, 'Pierre', ${DateTime.parse("1987-02-10").getMillis})")
    val result = statement.executeQuery("SELECT NAME, CREATION FROM USER WHERE NAME = 'Julie' ")
    result.next

    // Then
    result.getString(1) should be ("Julie")
    result.getLong(2) should be (DateTime.parse("1981-10-16").getMillis)
  }
}

trait H2Embedded extends FlatSpec with BeforeAndAfterEach {

  var db = new H2Database()

  override def beforeEach() {
    super.beforeEach()
    db.start()
  }

  override def afterEach() {
    super.afterEach()
    db.stop()
  }
}