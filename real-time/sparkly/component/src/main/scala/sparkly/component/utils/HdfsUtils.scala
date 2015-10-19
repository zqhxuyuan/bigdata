package sparkly.component.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.io.{DataOutputStream, ByteArrayOutputStream, ByteArrayInputStream, DataInputStream}
import sparkly.core.Context


object HdfsHelper {

  def delete(path: Path, conf: Configuration) = {
    val fs = getFileSystemForPath(path, conf)
    fs.delete(path, true)
  }

  def getOutputStream(path: Path, conf: Configuration): FSDataOutputStream = {
    val dfs = getFileSystemForPath(path, conf)
    // If the file exists and we have append support, append instead of creating a new file
    val stream: FSDataOutputStream = {
      if (dfs.isFile(path)) {
        if (conf.getBoolean("hdfs.append.support", false)) {
          dfs.append(path)
        } else {
          throw new IllegalStateException("File exists and there is no append support!")
        }
      } else {
        dfs.create(path)
      }
    }
    stream
  }

  def getInputStream(path: Path, conf: Configuration): FSDataInputStream = {
    val dfs = getFileSystemForPath(path, conf)
    dfs.open(path)
  }

  def getFileSystemForPath(path: Path, conf: Configuration): FileSystem = {
    // For local file systems, return the raw local file system, such calls to flush()
    // actually flushes the stream.
    val fs = path.getFileSystem(conf)
    fs match {
      case localFs: LocalFileSystem => localFs.getRawFileSystem
      case _ => fs
    }
  }
}


class SerializableHadoopConfiguration(val bytes: Array[Byte]) extends Serializable {

  def get(): Configuration = {
    val conf = new Configuration()
    conf.readFields(new DataInputStream(new ByteArrayInputStream(bytes)))
    conf
  }
}

object SerializableHadoopConfiguration {
  def apply(conf: Configuration): SerializableHadoopConfiguration = {
    val buffer = new ByteArrayOutputStream()
    conf.write(new DataOutputStream(buffer))
    buffer.flush()
    new SerializableHadoopConfiguration(buffer.toByteArray)
  }

  def from(context: Context): SerializableHadoopConfiguration = {
    SerializableHadoopConfiguration(context.hadoopConfiguration)
  }
}