package uk.ac.cdrc.data.spark.jdbc

/**
  * A writer utility for DataFrame to PostgreSQL tables
  */
import java.sql.Connection

import org.apache.spark.sql.DataFrame
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

case class PGTableWriter(cf: () => Connection){

  val nullString: String = "null"
  val delimiter: String = """\t"""

  val getSQLType: Map[String, String] = Map(
    "StringType" -> "text",
    "DateType" -> "date",
    "TimestampType" -> "timestamp",
    "FloatType" -> "real",
    "DoubleType" -> "double precision",
    "IntegerType" -> "int",
    "LongType" -> "bigint"
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

  def write(frame: DataFrame, table: String, overwrite: Boolean = false, index: Boolean = true, appendIdCol: Option[String] = Some("id"), numPartition: Int = 100): Unit = {

    if (overwrite)
      dropTable(table)
    createTable(frame, table)
    val gathered =frame.coalesce(numPartition)
    val isString = frame.dtypes.map(x => x._2 == "StringType")

    // Beware: this will open a db connection for every partition of your DataFrame.
    gathered.foreachPartition { rows =>
      val conn = cf()
      val cm = new CopyManager(conn.asInstanceOf[BaseConnection])

      cm.copyIn(
        Streamer.copyStatement(table),
        Streamer.rowsToInputStream(rows, isString))

      conn.close()
    }
    if (index)
      addIndices(table, gathered.columns)
    // Append an id column if required (e.g., when one needs spark's partitioning by IDs
    appendIdCol match {
      case Some(idColName) => addIdColumn(table, idColName)
      case _ => ()
    }
  }

  override def toString = s"PGTableWriter"
}
