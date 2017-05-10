package uk.ac.cdrc.data.spark.jdbc

/**
  * A writer utility for DataFrame to PostgreSQL tables
  */
import java.io.InputStream
import java.sql.Connection

import org.apache.spark.sql.{DataFrame, Row}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

class PGTableWriter(cf: () => Connection){

  // Convert every partition (an `Iterator[Row]`) to bytes (InputStream)
  def rowsToInputStream(rows: Iterator[Row], delimiter: String): InputStream = {

    val bytes: Iterator[Byte] = for {
      row <- rows
      b <- (row.mkString(delimiter) + "\n").getBytes
    } yield b

    new InputStream {
      override def read(): Int = if (bytes.hasNext) {
        bytes.next & 0xff // bitwise AND - make the signed byte an unsigned int from 0-255
      } else {
        -1
      }
    }
  }

  val getSQLType: Map[String, String] = Map(
    "StringType" -> "text",
    "DateType" -> "date",
    "TimestampType" -> "timestamp",
    "FloatType" -> "real",
    "DoubleType" -> "real",
    "IntegerType" -> "int",
    "LongType" -> "int"
  )

  def schemaToTable(frame: DataFrame, table: String): String = {
    val colDef = (for {
      (name, typ) <- frame.dtypes
    } yield s"$name ${getSQLType(typ)}") mkString ", "
    s"CREATE TABLE IF NOT EXISTS $table ($colDef);"
  }

  def dropTable(table: String): Unit = {
    val conn = cf()
    val stmt = conn.createStatement()
    stmt.execute(s"DROP TABLE IF EXISTS $table")
    conn.close()
  }

  def deleteData(table: String): Int = {
    val conn = cf()
    val stmt = conn.createStatement()
    val cnt = stmt.executeUpdate(s"DELETE from $table")
    conn.close()
    cnt
  }

  def createTable(frame: DataFrame, table: String): Unit = {
    val conn = cf()
    val stmt = conn.createStatement()
    stmt.execute(schemaToTable(frame, table))
    conn.close()
  }

  def addIdColumn(table: String, idColName: String): Unit =  {
    val conn = cf()
    val stmt = conn.createStatement()
    stmt.executeUpdate(s"ALTER TABLE $table add column $idColName serial primary key")
    conn.close()
  }

  def addIndices(table: String, cols: Seq[String] = Seq.empty): Unit = {
    val conn = cf()
    val createIndicesStmt = (for (col <- cols) yield s"CREATE INDEX ${table}_${col}_ix on $table($col)") mkString "; "
    conn.createStatement().executeUpdate(createIndicesStmt)
    conn.close()
  }

  def write(frame: DataFrame, table: String, overwrite: Boolean = false, index: Boolean = true, appendIdCol: Option[String] = Some("id")): Unit = {

    if (overwrite)
      dropTable(table)
    createTable(frame, table)

    // Beware: this will open a db connection for every partition of your DataFrame.
    frame.foreachPartition { rows =>
      val conn = cf()
      val cm = new CopyManager(conn.asInstanceOf[BaseConnection])

      cm.copyIn(
        s"""COPY $table FROM STDIN WITH (NULL 'null', FORMAT CSV, DELIMITER E'\t')""", // adjust COPY settings as you desire, options from https://www.postgresql.org/docs/9.5/static/sql-copy.html
        rowsToInputStream(rows, "\t"))

      conn.close()
    }
    if (index)
      addIndices(table, frame.columns)
    // Append an id column if required (e.g., when one needs spark's partitioning by IDs
    appendIdCol match {
      case Some(idColName) => addIdColumn(table, idColName)
      case _ => ()
    }
  }

  override def toString = s"PGTableWriter"
}
