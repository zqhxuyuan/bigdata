package sparkly.component.source

import java.io.{File, FileWriter}
import java.nio.file.Files

import sparkly.testing._
import sparkly.core._

class CsvFileDirectorySourceSpec extends ComponentSpec {

  var workingDirectory: File = null

  override def beforeEach() {
    super.beforeEach()
    workingDirectory = Files.createTempDirectory("sparkly-sparkly-test-working-directory").toFile
  }

  "Csv files source" should "stream csv file" in {
    write("data.csv", List("Julie;1981;Female", "Pierre;1987;Male"))

    val configuration = ComponentConfiguration (
      clazz = classOf[CsvFileDirectorySource].getName,
      name = "Csv source",
      properties = Map (
        "Directory" -> workingDirectory.getAbsolutePath
      ),
      outputs = Map (
        "Instances" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("Name", "Birth date", "Gender")))
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").features should contain only (
        Map("Name" -> "Julie", "Birth date" -> "1981", "Gender" -> "Female"),
        Map("Name" -> "Pierre", "Birth date" -> "1987", "Gender" -> "Male")
      )
    }
  }

  "Csv files source" should "filter not matched file" in {
    write("data.tsv", List("Julie;32", "Pierre;28"))
    write("data.csv", List("Julie;1981;Female", "Pierre;1987;Male"))

    val configuration = ComponentConfiguration (
      clazz = classOf[CsvFileDirectorySource].getName,
      name = "Csv source",
      properties = Map (
        "Directory" -> workingDirectory.getAbsolutePath,
        "Filename pattern" -> "*.csv"
      ),
      outputs = Map (
        "Instances" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("Name", "Birth date", "Gender")))
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").features should contain only (
        Map("Name" -> "Julie", "Birth date" -> "1981", "Gender" -> "Female"),
        Map("Name" -> "Pierre", "Birth date" -> "1987", "Gender" -> "Male")
      )
    }
  }

  def write(filename: String, values: List[String]) {
    val workingFile = new File(workingDirectory, filename)
    workingFile.delete()

    val writer = new FileWriter(workingFile)
    values.foreach(line => writer.write(s"${line}\n"))
    writer.close()
  }

}
