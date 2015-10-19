package sparkly.common

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.objenesis.strategy.StdInstantiatorStrategy

class KryoSerializer[T](initialBufferSize: Int = 256, maxBufferSize: Int = 16 * 1024) extends Serializer[T] {

  override def serialize(value: T): Array[Byte] = {
    val kryo = KryoSerializer.createKryo()

    val output = new Output(initialBufferSize, maxBufferSize)

    kryo.writeClassAndObject(output, value)
    output.close()

    output.getBuffer()
  }

  def deserialize(bytes: Array[Byte]): T = {
    val kryo = KryoSerializer.createKryo()

    val input = new Input(bytes)
    kryo.readClassAndObject(input).asInstanceOf[T]
  }
}

object KryoSerializer {
  def createKryo(): Kryo = {
    val kryo = new Kryo()
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy())

    kryo
  }
}
