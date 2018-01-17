package com.lppz.spark.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.fs.FileSystem
import com.lppz.spark.scala.jdbc.MysqlSpark

class CsvHandler {
  
  def importCsv2Spark(sc:SparkContext){
    val sqlContext = new SQLContext(sc)
    
    val d=sqlContext.read.format("com.databricks.spark.csv").option("header", false)
//      .option("inferSchema", true.toString())
      .load("/home/yulei/AZPWSD01300.csv")
      
    d.rdd.saveAsTextFile("/tmp/csv/1111")
    
    var hdfs:FileSystem=null
    
    hdfs=new SparkHdfsUtil().getFileSystem("hdfs://hamaster:9000")
    
    val destFileName = "hdfs://hamaster:9000/tmp/csv/fuck.txt";
    val mysql=new MysqlSpark()
    mysql.mergeHdfsFile(hdfs, destFileName, "/tmp/csv/1111", "hdfs://hamaster:9000")
  }
}