package sparkly.dao

import org.mapdb._
import java.io.File

import scala.collection.JavaConversions._
import sparkly.config.SparklyConfig.DB_DIRECTORY

abstract class Database[K, V](name :String) {

  val map = Database.db.getHashMap[K, V](name);

  def get(id: String): Option[V] = Option(map.get(id))

  def all(): List[V] = map.values.toList

  def store(key: K, value: V): Unit = {
    map.put(key, value)
    Database.db.commit()
  }

  def delete(key: K): Unit = {
    map.remove(key)
    Database.db.commit()
  }
}

object Database {
  new File(DB_DIRECTORY).mkdirs()
  val db = DBMaker.newFileDB(new File(DB_DIRECTORY, "data")).make()
}
