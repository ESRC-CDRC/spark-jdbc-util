package uk.ac.cdrc.data.spark.jdbc

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
/**
  * Test PGTableWriter
  */
class StreamerSpec extends FlatSpec with Matchers {

  "PGTableWriter" should "quote the rows" in {
    val r = Row(1, "aaa\"aaa")
    val stream = Streamer.rowsToInputStream(Iterator(r), Array(false, true), "\t")
    scala.io.Source.fromInputStream(stream).mkString.stripLineEnd should be ("1\t\"aaa\"\"aaa\"")
  }
}
