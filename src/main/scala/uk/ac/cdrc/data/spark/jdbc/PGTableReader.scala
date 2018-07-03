package uk.ac.cdrc.data.spark.jdbc

import org.apache.spark.sql._

/**
  * Created on 5/10/17.
  */

class PGTableReader(reader: DataFrameReader) {

  def getIdBound(table: String, idCol: String): (Long, Long) = {
    reader.option("dbtable", s"(select min($idCol) as minId, max($idCol) as maxId from $table) idbound").
      load().collect()(0) match {
      case Row(minId: Int, maxId: Int) => (minId, maxId)
      case Row(minId: Long, maxId: Long) => (minId, maxId)
      case Row(null, null) => (0, 0)
    }
  }

  def readTable(table: String, idCol: String = "id", numPartitions: Int = 100, options: Map[String, String] = Map.empty): PGTable = {
    val idBound = getIdBound(table, idCol)
    val df = reader.option("dbtable", table).
      option("numPartitions", numPartitions).
      option("partitionColumn", idCol).
      option("lowerBound", idBound._1).
      option("upperBound", idBound._2).
      options(options).
      load().cache
    PGTable(df, table, idCol)
  }
}
