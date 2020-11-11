package com.zeotap.retrigger.utility

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


class PostgresDatasetProvider {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def getMappingDataset(dbProps: Properties, mappingTableName: String)(implicit spark: SparkSession): DataFrame = {

    try {
      dbProps.put("driver","org.postgresql.Driver")
      val url = "jdbc:postgresql://" + dbProps.get("host") + ":" + "5432" + "/" + dbProps.get("db")
      spark.read.jdbc(url, mappingTableName, dbProps)
    }
    catch {
      case exception: Exception => {
        log.error("Error while postgres mapping load : " + exception)
        throw new Exception("Error while reading data from postgres SQL")
      }
    }
  }

  def getConnection(prop: Properties): Connection = {
    try {
        Class.forName("org.postgresql.Driver").newInstance()
        log.info("Connecting to the selected database...")
        val url = "jdbc:postgresql://" + prop.get("host") + ":" + "5432" + "/" + prop.get("db")
        val  connection = DriverManager.getConnection(url, prop.getProperty("user"), prop.getProperty("password"))
        log.info("Connected database successfully...")
      connection
    }
    catch {
      case e: SQLException => {
        log.error("sql Exception" + e.printStackTrace())
        throw e
      }
    }
  }

  def updateDataset(databaseProps: Properties, list:String): Unit = {

  try {
    val connection = getConnection(databaseProps)
    connection.setAutoCommit(true)
    val pstmt = connection.prepareStatement(s"UPDATE dp_job_status set status='RUNNING' where job_id IN (${list})")
    val result = pstmt.executeUpdate();
    log.info("Result set value: " + result.toString + "Updated counts" + pstmt.getUpdateCount)
    pstmt.close()
    connection.close()

  }catch {
    case e: SQLException =>
      log.error("Error while upsert " + e.printStackTrace())
      throw e
  }
  }
}
