package com.lppz.spark.mysql.scala;

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
import java.util.Properties
import com.lppz.spark.scala.jdbc.MysqlSpark
import com.lppz.spark.scala.jdbc.SparkJdbcTemplete
import com.lppz.spark.scala.HiveContextUtil
import com.lppz.spark.scala.SparkHdfsUtil
import com.lppz.spark.mysql.bean.Hive2DbmsBean

class InitAccMemberHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName)
  
  def initAccMember(appName: String,bean:Hive2DbmsBean,sc:SparkContext,lowerBound:Long,upperBound:Long,ds:String)={
    val sqlContext = new SQLContext(sc)
    
    val prop = new Properties()
    prop.put("user", bean.getRdbmsJdbcUser)
    prop.put("password", bean.getRdbmsJdbcPasswd)
    
    var df=sqlContext.read.jdbc(bean.getRdbmsJdbcUrl, bean.getRdbmsTableName, bean.getPrimaryKey, lowerBound, upperBound, bean.getPartition, prop)
    
//    if(df.rdd.isEmpty())
//      null
    
    df.createOrReplaceTempView(bean.getRdbmsTableName.replaceAll("/", ""))
    
    df=df.sqlContext.sql("select * from ".concat(bean.getRdbmsTableName.replaceAll("/", "").concat(" where ")
        .concat(bean.getPrimaryKey).concat(" between ").concat(String.valueOf(lowerBound)).concat(" and ").concat(String.valueOf(upperBound))))
    
    df.rdd.cache()
    
    val typedata="NULL"
    val fileName = bean.getHdfsUrl + "/tmp/mysqlhive/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/" + lowerBound + "_" + upperBound
    df.rdd.map { r =>
      {
        val s: StringBuilder = new StringBuilder("")
        for (j <- Range(0, r.length)) {
          var o = r.get(j)
          if (o == null)
            o = typedata;
          if (o.isInstanceOf[String]) {
            if ("null".equals(o.asInstanceOf[String].toLowerCase()))
              o = typedata
            o = (o.asInstanceOf[String]).replaceAll("\r", "")
            o = (o.asInstanceOf[String]).replaceAll("\n", "")
            o = (o.asInstanceOf[String]).replaceAll("\t", " ")
          }
          s.append(o)
          if (j < r.length - 1)
            s.append("\t")
        }
        s
      }
    }.saveAsTextFile(fileName)    
    
    var hdfs:FileSystem=null
    val mysql=new MysqlSpark()
    hdfs=new SparkHdfsUtil().getFileSystem(bean.getHdfsUrl)
    val destFileName = bean.getHdfsUrl + "/tmp/mysqlGen/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/" + lowerBound + "_" + upperBound+".txt"
    mysql.mergeHdfsFile(hdfs, destFileName, fileName, bean.getHdfsUrl)
  }
  
}