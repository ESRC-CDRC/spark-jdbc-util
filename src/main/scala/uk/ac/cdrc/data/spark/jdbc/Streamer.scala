package uk.ac.cdrc.data.spark.jdbc

import java.io.InputStream

import org.apache.spark.sql.Row

/**
  * Row serialization for PostgreSQL copy command
  */
object Streamer {

  // Convert every partition (an `Iterator[Row]`) to bytes (InputStream)
  def rowsToInputStream(rows: Iterator[Row], isString: Array[Boolean], delimiter: String): InputStream = {

    val bytes: Iterator[Byte] = for {
      row <- rows
      quoted = row.toSeq.zip(isString).map {x =>
        if (x._2)
          s""""${"\"".r replaceAllIn (if (x._1 == null) "" else x._1.toString, "\"\"")}""""
        else if (x._1 == null) "" else x._1.toString}
      b <- (quoted.mkString(delimiter) + "\n").getBytes
    } yield b

    new InputStream {
      override def read(): Int = if (bytes.hasNext) {
        bytes.next & 0xff // bitwise AND - make the signed byte an unsigned int from 0-255
      } else {
        -1
      }
    }
  }
}
