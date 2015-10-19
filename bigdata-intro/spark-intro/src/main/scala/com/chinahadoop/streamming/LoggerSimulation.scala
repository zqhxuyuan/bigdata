package com.chinahadoop.streamming

import java.io.{PrintWriter, OutputStreamWriter, IOException, BufferedOutputStream}
import java.util.concurrent.{TimeUnit, Executors}
import java.net.ServerSocket

/**
 * Chen Chao
 * 模拟随记发送A-G 7个字母中的一个，时间间隔可以指定
 */
object LoggerSimulation {

  def generateContent(index: Int): String = {
    import scala.collection.mutable.ListBuffer
    val charList = ListBuffer[Char]()
    for (i <- 65 to 90) {
      charList += i.toChar
    }
    val charArray = charList.toArray
    charArray(index).toString
  }

  def index = {
    import java.util.Random
    val rdm = new Random
    //rdm.nextInt(26)
    rdm.nextInt(7)
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: <port> <millisecond>")
      System.exit(1)
    }

    val listener = new ServerSocket(args(0).toInt)
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          while (true) {
            Thread.sleep(args(1).toLong)
            val content = generateContent(index)
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}