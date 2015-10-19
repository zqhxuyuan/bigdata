package sparkly.component.source

import sparkly.testing._
import sparkly.core._

class JdbcSourceSpec extends ComponentSpec with H2Embedded {

  "JDBC source" should "stream data from H2 database" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[JdbcSource].getName,
      name = "H2 source",
      properties = Map (
        "Url" -> db.connectUrl,
        "Driver" -> "org.h2.Driver",
        "User" -> "sa",
        "Password" -> "sa",
        "SQL query" -> "SELECT ID, NAME, AGE FROM USER WHERE $CONDITIONS",
        "Increment field" -> "ID",
        "Start value" -> "1"
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(selectedFeatures = Map("Fields" -> List("id", "name", "age")))
      )
    )

    val connection = JdbcUtil.getConnection("org.h2.Driver", db.connectUrl, "sa", "sa")
    val statement = connection.createStatement()
    statement.execute("DROP TABLE IF EXISTS USER")
    statement.execute("CREATE TABLE USER (ID INT PRIMARY KEY, NAME VARCHAR, AGE INT)")
    statement.execute("INSERT INTO USER (ID, NAME, AGE) VALUES (1, 'Brice', 33)")
    statement.execute("INSERT INTO USER (ID, NAME, AGE) VALUES (2, 'Julie', 33)")
    statement.execute("INSERT INTO USER (ID, NAME, AGE) VALUES (3, 'Pierre', 27)")


    // When
    val component = deployComponent(configuration)

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("id" -> "2", "name" -> "Julie", "age" -> "33"),
        Map("id" -> "3", "name" -> "Pierre", "age" -> "27")
      )
    }

    component.outputs("Output").instances.clear()
    statement.execute("INSERT INTO USER (ID, NAME, AGE) VALUES (0, 'Fabien', 29)")
    statement.execute("INSERT INTO USER (ID, NAME, AGE) VALUES (4, 'Julie', 27)")

    eventually {
      component.outputs("Output").features should contain only (
        Map("id" -> "4", "name" -> "Julie", "age" -> "27")
      )
    }
  }

}
