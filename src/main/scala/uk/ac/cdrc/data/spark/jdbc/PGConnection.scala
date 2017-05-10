package uk.ac.cdrc.data.spark.jdbc

import java.sql.Connection

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.{DataFrameReader, SparkSession}

/**
  * A class for connection object
  * @param session Spark session
  * @param url jdbc:postgres://host:port/db
  * @param user username
  * @param password password
  */
class PGConnection(session: SparkSession, url: String, user: String, password: String) {

  private val connProps = Map(
    "url" -> url,
    "user" -> user,
    "password" -> password
  )

  private val connectionProperties: JDBCOptions = {
    val options = Map("driver" -> "org.postgresql.Driver", "url" -> url, "dbtable" -> "__dummy__")
    new JDBCOptions(options ++ connProps)
  }

  private def reader: DataFrameReader = session.read.
    format("jdbc").
    options(connProps)


  // Spark reads the "driver" property to allow users to override the default driver selected, otherwise
  // it picks the Redshift driver, which doesn't support JDBC CopyManager.
  // https://github.com/apache/spark/blob/v1.6.1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L44-51
  val cf: () => Connection = JdbcUtils.createConnectionFactory(connectionProperties)
  def getTableReader = new PGTableReader(reader)
  def getTableWriter = new PGTableWriter(cf)
}
