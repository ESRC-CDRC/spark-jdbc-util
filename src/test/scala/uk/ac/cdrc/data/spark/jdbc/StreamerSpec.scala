package uk.ac.cdrc.data.spark.jdbc

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
/**
  * Test PGTableWriter
  */
class StreamerSpec extends FlatSpec with Matchers {

  "Streamer" should "quote the rows" in {
    val r = Row(1, "aaa\"aaa")
    val stream = Streamer.rowsToInputStream(Iterator(r), Array(false, true))
    scala.io.Source.fromInputStream(stream).mkString.stripLineEnd should be (s"""1${Streamer.delimiter}"aaa""aaa"""")
  }

  it should "deal with null values" in {
    val r = Row(1, null)
    val stream1 = Streamer.rowsToInputStream(Iterator(r), Array(false, true))
    scala.io.Source.fromInputStream(stream1).mkString.stripLineEnd should be (s"1${Streamer.delimiter}null")
    val stream2 = Streamer.rowsToInputStream(Iterator(r), Array(false, false))
    scala.io.Source.fromInputStream(stream2).mkString.stripLineEnd should be (s"1${Streamer.delimiter}null")
  }
}
