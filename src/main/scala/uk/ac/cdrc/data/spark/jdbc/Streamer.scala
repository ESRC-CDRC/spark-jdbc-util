package uk.ac.cdrc.data.spark.jdbc

import java.io.InputStream

import org.apache.spark.sql.Row

/**
  * Row serialization for PostgreSQL copy command
  */

trait Streamer {
  val nullString: String = """null"""
  val delimiter: String = """\t"""

  // adjust COPY settings as you desire, options from https://www.postgresql.org/docs/9.5/static/sql-copy.html
  def copyStatement(table: String): String = s"""COPY \$table FROM STDIN WITH (NULL '$nullString', FORMAT CSV, DELIMITER E'$delimiter')"""

  // Convert every partition (an `Iterator[Row]`) to bytes (InputStream)
  def rowsToInputStream(rows: Iterator[Row], isString: Array[Boolean]): InputStream = {

    val bytes: Iterator[Byte] = for {
      row <- rows
      quoted = row.toSeq.zip(isString).map {x =>
        if (x._1 == null) nullString
        else if (x._2) s""""${"\"".r replaceAllIn (x._1.toString, "\"\"")}""""
        else x._1.toString
      }
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


object Streamer extends Streamer
