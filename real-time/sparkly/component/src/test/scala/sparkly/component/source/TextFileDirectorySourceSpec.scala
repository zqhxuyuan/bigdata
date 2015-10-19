package sparkly.component.source

import java.io.{File, FileWriter}
import java.nio.file.Files

import sparkly.testing._
import sparkly.core._

class TextFileDirectorySourceSpec extends ComponentSpec {

  var workingDirectory: File = null

  override def beforeEach() {
    super.beforeEach()
    workingDirectory = Files.createTempDirectory("sparkly-sparkly-test-working-directory").toFile
  }

  "Text file directory" should "stream text file" in {
    write("data.txt", List("the cow jumped over the moon", "the man went to the store and bought some candy"))

    val configuration = ComponentConfiguration (
      clazz = classOf[TextFileDirectorySource].getName,
      name = "Text source",
      properties = Map (
        "Directory" -> workingDirectory.getAbsolutePath
      ),
      outputs = Map (
        "Instances" -> StreamConfiguration(mappedFeatures = Map("Line" -> "Sentence"))
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").features should contain only (
        Map("Sentence" -> "the cow jumped over the moon"),
        Map("Sentence" -> "the man went to the store and bought some candy")
      )
    }
  }

  "Text file directory" should "filter not matched file" in {
    write("data.csv", List("Julie;32", "Pierre;28"))
    write("data.txt", List("the cow jumped over the moon", "the man went to the store and bought some candy"))

    val configuration = ComponentConfiguration (
      clazz = classOf[TextFileDirectorySource].getName,
      name = "Text source",
      properties = Map (
        "Directory" -> workingDirectory.getAbsolutePath,
        "Filename pattern" -> "*.txt"
      ),
      outputs = Map (
        "Instances" -> StreamConfiguration(mappedFeatures = Map("Line" -> "Sentence"))
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").features should contain only (
        Map("Sentence" -> "the cow jumped over the moon"),
        Map("Sentence" -> "the man went to the store and bought some candy")
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
