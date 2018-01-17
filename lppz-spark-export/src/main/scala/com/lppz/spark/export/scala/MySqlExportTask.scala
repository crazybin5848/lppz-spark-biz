package com.lppz.spark.export.scala;

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import java.util.Arrays
import org.apache.spark.sql.Row
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList
import org.apache.spark.sql.SQLContext
import java.util.Properties
import com.lppz.spark.scala.jdbc.MysqlSpark
import com.lppz.spark.scala.jdbc.SparkJdbcTemplete
import com.lppz.spark.export.bean.Hive2DbmsBean
import com.lppz.spark.scala.HiveContextUtil
import com.lppz.spark.scala.SparkHdfsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import com.lppz.spark.util.SparkYamlUtils
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import com.lppz.spark.export.BuildLoadFileUtil

object MySqlExportTask extends Serializable {
  @transient lazy val LOG = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("MySqlExportTask")
      .setMaster(args(1))
    val sc = new MysqlSpark().buildSc("MySqlExportTask", args(1))
    val bclass = new Hive2DbmsBean().getClass()
    val bean = SparkYamlUtils.loadYaml(args(0), false, bclass)
    val calen3 = Calendar.getInstance()
    calen3.add(Calendar.MONTH, -3)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val timestr = sdf.format(new Date().getTime()).toString()
    val currmonth = timestr.substring(0, 6)
    val threemonthago = sdf.format(calen3.getTime()).toString()
    val monthago = threemonthago.substring(0, 6)
    
    HiveContextUtil.exec(sc, "use " + bean.getHiveSchema)

    val sql2 = bean.getHiveSql
    val dataframe = HiveContextUtil.getRDD(sc, sql2)

    val fileName = "/tmp/exportorder/"+bean.getHiveTableName +".txt"
    val destFileName = bean.getHdfsUrl + "/tmp/Genexportorder/"+bean.getHiveTableName+".txt"
    val util = new BuildLoadFileUtil()
    dataframe.rdd.repartition(bean.getPartition).map { row =>
      val sql = util.buildLoadFile(row)
      sql
    }.saveAsTextFile(fileName)

    val mysql = new MysqlSpark()

    var hdfs: FileSystem = null

    hdfs = new SparkHdfsUtil().getFileSystem(bean.getHdfsUrl)

    mysql.mergeHdfsFile(hdfs, destFileName, fileName, bean.getHdfsUrl)

  }

}