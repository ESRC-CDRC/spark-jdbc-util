package uk.ac.cdrc.data.spark.jdbc

import org.apache.spark.sql._

/**
  * Created on 5/10/17.
  */

class PGTableReader(session: SparkSession, url: String, user: String, password: String) {

  def reader: DataFrameReader = session.read.
    format("jdbc").
    option("url", url).
    option("user", user).
    option("password", password)

  def getIdBound(table: String, idCol: String): (Long, Long) = {
    reader.option("dbtable", s"(select min($idCol) as minId, max($idCol) as maxId from $table) idbound").
      load().collect()(0) match {
      case Row(minId: Int, maxId: Int) => (minId, maxId)
      case Row(minId: Long, maxId: Long) => (minId, maxId)
    }
  }

  def readTable(table: String, idCol: String = "id", numPartitions: Int = 100, options: Map[String, String] = Map.empty): DataFrame = {
    val idBound = getIdBound(table, idCol)
    reader.option("dbtable", table).
      option("numPartitions", numPartitions).
      option("partitionColumn", idCol).
      option("lowerBound", idBound._1).
      option("upperBound", idBound._2).
      options(options).
      load().cache
  }
}
