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

object HanaIncreSqlExportTask extends Serializable {
  @transient lazy val LOG = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("HanaIncreSqlExportTask")
      .setMaster(args(1))
    val sc = new MysqlSpark().buildSc("HanaIncreSqlExportTask", args(1))
    val bclass = new Hive2DbmsBean().getClass()
    val bean = SparkYamlUtils.loadYaml(args(0), false, bclass)
    val calen3 = Calendar.getInstance()
    calen3.add(Calendar.DATE, -6)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val timestr = sdf.format(calen3.getTime()).toString()
    
    HiveContextUtil.exec(sc, "use " + bean.getHiveSchema)

    val sql2 = bean.getHiveSql
    val df = HiveContextUtil.getRDD(sc, sql2)

    df.rdd.cache()
    
    var fileName= bean.getHdfsUrl + "/tmp/hana2hive/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/"+ timestr
    df.rdd.map { r =>
        {
          val s: StringBuilder = new StringBuilder("")
          for (j <- Range(0, r.length)) {
            var o = r.get(j)
            if (o == null || o.toString().trim().equals(""))
              o = "NULL";
            if (o.isInstanceOf[String]) {
              if ("null".equals(o.asInstanceOf[String].toLowerCase()))
                o = "NULL";
              o = (o.asInstanceOf[String]).replaceAll("\r", "")
              o = (o.asInstanceOf[String]).replaceAll("\n", "")
              o = (o.asInstanceOf[String]).replaceAll("\t", " ")
            }
            s.append(o);
            if (j < r.length - 1)
              s.append("\t")
          }
          s
        }
      }.saveAsTextFile(fileName)
      
      val mysql=new MysqlSpark()
        
      var hdfs:FileSystem=null
      
      hdfs=new SparkHdfsUtil().getFileSystem(bean.getHdfsUrl)
      
      val destFileName = bean.getHdfsUrl + "/tmp/hanaGen/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/" +timestr+".txt"
      mysql.mergeHdfsFile(hdfs, destFileName, fileName, bean.getHdfsUrl) 
      
      val sql = "load data inpath '" + destFileName+"' into table " + bean.getHiveTableName +" PARTITION(ds=\""+timestr+"\")";


  }

}