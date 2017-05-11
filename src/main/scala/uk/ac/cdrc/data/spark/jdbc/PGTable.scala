package uk.ac.cdrc.data.spark.jdbc

import org.apache.spark.sql.DataFrame

/**
  * A wrapper class of data frame
  * @param df the wrapped data frame
  */
case class PGTable(df: DataFrame, name: String, idCol: String){
  private val nameDelimiter = "\\.".r
  def simpleName: String = nameDelimiter.split(name).last
}

object PGTable {
  implicit def toDataFrame(table: PGTable): DataFrame = table.df
}
