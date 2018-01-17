package com.lppz.spark.export.scala;

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
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
import org.apache.spark.sql.DataFrame
import java.util.Properties
import com.lppz.spark.scala.jdbc.MysqlSpark
import com.lppz.spark.scala.jdbc.SparkJdbcTemplete
import com.lppz.spark.export.bean.Hive2DbmsBean
import com.lppz.spark.scala.HiveContextUtil
import com.lppz.spark.scala.SparkHdfsUtil
import com.lppz.spark.export.handler.HaNaJdbcHandler
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.HashMap

class ExportHanaDataHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName) 
  
  
  def exportHaNaTableWithPart(appName: String,bean:Hive2DbmsBean,sc:SparkContext,lowerBound:Long,upperBound:Long)={
    val sqlContext = new SQLContext(sc)
    
    val prop = new Properties()
    prop.put("user", bean.getRdbmsJdbcUser)
    prop.put("password", bean.getRdbmsJdbcPasswd)
    
    var df=sqlContext.read.jdbc(bean.getRdbmsJdbcUrl, bean.getRdbmsSchemaName+".\""+bean.getRdbmsTableName+"\"", bean.getPrimaryKey, lowerBound, upperBound, bean.getPartition, prop)
    
    if(df.rdd.isEmpty())
      null
    
    df.createOrReplaceTempView(bean.getRdbmsTableName.replaceAll("/", ""))
    
    df=df.sqlContext.sql("select * from ".concat(bean.getRdbmsTableName.replaceAll("/", "").concat(" where ")
        .concat(bean.getPrimaryKey).concat(" between ").concat(String.valueOf(lowerBound)).concat(" and ").concat(String.valueOf(upperBound))))
    
    
    df.rdd.cache()
    
    //分桶
    var partitionList=new ArrayList[String]()
    df.select(bean.getPartitionColumn).distinct().rdd.collect().map { r => {
      partitionList.add(r.get(0).asInstanceOf[String])
      
    } }
    
    var hdfs:FileSystem=null
    
    val mysql=new MysqlSpark()
    
    hdfs=new SparkHdfsUtil().getFileSystem(bean.getHdfsUrl)
    
    val rtnSql=new ArrayList[String]()
    
    for(l<-Range(0, partitionList.size())){
      var p=partitionList.get(l)
      var fileName= bean.getHdfsUrl + "/tmp/hanahive/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/"
      fileName += p+"/" + lowerBound + "_" + upperBound
      
      val destFileName = bean.getHdfsUrl + "/tmp/hanaGen/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/" +p+"/"+ lowerBound + "_" + upperBound+".txt"
      
      df.filter(bean.getPartitionColumn+"="+p).rdd.repartition(bean.getPartition).map { r =>
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
      
      mysql.mergeHdfsFile(hdfs, destFileName, fileName, bean.getHdfsUrl) 
      
      val sql = "load data inpath '" + destFileName+"' into table " + bean.getHiveTableName +" PARTITION(ds=\""+p+"\")";
      
      rtnSql.add(sql)
    }
    
    rtnSql
  }
  
  def exportHaNaTableWithRowid(appName: String,bean:Hive2DbmsBean,sc:SparkContext,lowerBound:Long,upperBound:Long,ds:String)={
    val sqlContext = new SQLContext(sc)
    
    val prop = new Properties()
    prop.put("user", bean.getRdbmsJdbcUser)
    prop.put("password", bean.getRdbmsJdbcPasswd)
    //var df:DataFrame = null

      var df=sqlContext.read.jdbc(bean.getRdbmsJdbcUrl, bean.getRdbmsSchemaName+".\""+bean.getRdbmsTableName+"\"", prop)
      
      //val cond = bean.getWhereCond.concat("=\"").concat(ds).concat("\"")
      //df.where(cond).show()
    //if(df.rdd.isEmpty())
      //null
      df.createOrReplaceTempView(bean.getRdbmsTableName.replaceAll("/", ""))
    
    df=df.sqlContext.sql("select * from ".concat(bean.getRdbmsTableName.replaceAll("/", "").concat(" where ")
        .concat(bean.getWhereCond).concat(" = \"").concat(ds).concat("\"")))
    //df.show()
    df.rdd.cache()
    
    var fileName= bean.getHdfsUrl + "/tmp/hana2hive/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/"+ ds
    df.rdd.repartition(bean.getPartition).map { r =>
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
      
      val destFileName = bean.getHdfsUrl + "/tmp/hanaGen/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/" +ds+".txt"
      mysql.mergeHdfsFile(hdfs, destFileName, fileName, bean.getHdfsUrl) 
      
      val sql = "load data inpath '" + destFileName+"'overwrite into table " + bean.getHiveTableName +" PARTITION(ds=\""+ds+"\")";
      
      sql
  }
  
  def exportIncreHaNaTable(appName: String,bean:Hive2DbmsBean,sc:SparkContext,lowerBound:Long,upperBound:Long,ds:String)={
    val sqlContext = new SQLContext(sc)
    
    val prop = new Properties()
    prop.put("user", bean.getRdbmsJdbcUser)
    prop.put("password", bean.getRdbmsJdbcPasswd)
    
    var df=sqlContext.read.jdbc(bean.getRdbmsJdbcUrl, bean.getRdbmsSchemaName+".\""+bean.getRdbmsTableName+"\"", bean.getPrimaryKey, lowerBound, upperBound, bean.getPartition, prop)
    if(df.rdd.isEmpty())
      null
    
    df.createOrReplaceTempView(bean.getRdbmsTableName.replaceAll("/", ""))
    
    df=df.sqlContext.sql("select * from ".concat(bean.getRdbmsTableName.replaceAll("/", "").concat(" where ")
        .concat(bean.getWhereCond).concat(" = \"").concat(ds).concat("\" and ")
        .concat(bean.getPrimaryKey).concat(" between ").concat(String.valueOf(lowerBound)).concat(" and ").concat(String.valueOf(upperBound))))

    df.rdd.cache()
    
    var fileName= bean.getHdfsUrl + "/tmp/hana2hive/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/"+ ds +"/"+ lowerBound + "_" + upperBound
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
      
      val destFileName = bean.getHdfsUrl + "/tmp/hanaGen/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/" +ds+"/"+ lowerBound + "_" + upperBound+".txt"
      mysql.mergeHdfsFile(hdfs, destFileName, fileName, bean.getHdfsUrl) 
      
      val sql = "load data inpath '" + destFileName+"' into table " + bean.getHiveTableName +" PARTITION(ds=\""+ds+"\")";
      
      sql
  }
  
  def exportFullHaNaTable(appName: String,bean:Hive2DbmsBean,sc:SparkContext,lowerBound:Long,upperBound:Long)={
    val sqlContext = new SQLContext(sc)
    
    val prop = new Properties()
    prop.put("user", bean.getRdbmsJdbcUser)
    prop.put("password", bean.getRdbmsJdbcPasswd)
    
    var df=sqlContext.read.jdbc(bean.getRdbmsJdbcUrl, bean.getRdbmsSchemaName+".\""+bean.getRdbmsTableName+"\"", bean.getPrimaryKey, lowerBound, upperBound, bean.getPartition, prop)
    
    if(df.rdd.isEmpty())
      null
    
    df.createOrReplaceTempView(bean.getRdbmsTableName.replaceAll("/", ""))
    
    df=df.sqlContext.sql("select * from ".concat(bean.getRdbmsTableName.replaceAll("/", "").concat(" where ")
        .concat(bean.getPrimaryKey).concat(" between ").concat(String.valueOf(lowerBound)).concat(" and ").concat(String.valueOf(upperBound))))
    
    
    df.rdd.cache()
    
    var fileName= bean.getHdfsUrl + "/tmp/hanahive/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/"+ lowerBound + "_" + upperBound
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
      
      val destFileName = bean.getHdfsUrl + "/tmp/hanaGen/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/" + lowerBound + "_" + upperBound+".txt"
      
      mysql.mergeHdfsFile(hdfs, destFileName, fileName, bean.getHdfsUrl) 
      val sql = "load data inpath '" + destFileName+"' into table " + bean.getHiveTableName ;
      sql
  }
  
  def exec(loadSql:ArrayList[String],sc:SparkContext,bean:Hive2DbmsBean){
    if(null==loadSql || 0==loadSql.size())
      return
      
    HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
    
    for(j<-Range(0,loadSql.size())){
      val sql=loadSql.get(j)
      
      HiveContextUtil.exec(sc,sql)
    }
  }
  
  def test(sc:SparkContext,bean:Hive2DbmsBean)={
    HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
    
    HiveContextUtil.execQuery(sc, "select * from azihyd0800 limit 1")
  }
}