package com.lppz.spark.scala

import java.util.ArrayList
import java.util.HashMap
import java.util.UUID
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.Column
import com.lppz.spark.bean.SparkMysqlDmlBean
import com.lppz.spark.bean.SparkSqlConfigBean
import java.sql.DriverManager
import org.apache.spark.sql.Row
import com.lppz.spark.util.SparkHiveUtil
import com.lppz.core.datasource.DynamicDataSource
import javax.sql.DataSource
import java.sql.PreparedStatement
import org.apache.log4j.Logger
import com.lppz.core.datasource.LppzBasicDataSource
import com.lppz.spark.scala.jdbc.MysqlSpark

/**
 * @author zoubin
 */
class HistoryOrderHandler extends Serializable{
   @transient lazy val LOG=Logger.getLogger(getClass.getName)
   
   def getMysqlListAndSave2Hdfs(appName: String, mode: String, hiveSchema: String, hiveTbName: String, hdfsUrl: String, ds: String, config: SparkSqlConfigBean, bean: SparkMysqlDmlBean, sc: SparkContext,dateDel:String,dataType:Boolean,isTest: Boolean) = {
    val mysql=new MysqlSpark()
    val df = if (StringUtils.isBlank(bean.getSql)) mysql.getMysqlListArrayDF(appName, mode, config, bean, sc) else mysql.buildDfByIdJdbc(appName, mode, config, bean, sc)  
    val url = if (isTest) "" else hdfsUrl
    val fileName = url + "/tmp/mysqlhive/" + hiveSchema + "/" + hiveTbName + "/" + ds + "/" + bean.getOffset + "_" + bean.getTotal4Once
    val typedata=if(dataType) "NULL" else "\\N"
    val rdd = df.rdd
    rdd.map { r =>
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
    rdd.cache()
    var hdfs:FileSystem=null
    if(!isTest){
    hdfs=new SparkHdfsUtil().getFileSystem(hdfsUrl)
    val destFileName = url + "/tmp/mysqlGen/" + hiveSchema + "/" + hiveTbName + "/" + ds + "/" + bean.getOffset + "_" + bean.getTotal4Once+".txt"
    mysql.mergeHdfsFile(hdfs, destFileName, fileName, hdfsUrl)
    }
    
    var i = 0
    val smap = new HashMap[String, String]()
    if (StringUtils.isBlank(bean.getColList))
      smap
    else {
      val coString = bean.getColList.split(",")
      val len=coString.length
      val colList: Array[Column] = new Array[Column](len)
      coString.foreach { x =>
        {
          colList.update(i, df.col(x))
          i += 1
        }
      }
      val delFileName = url + "/tmp/mysqlhive/" + hiveSchema + "del/" + "omsdeldetail" + "/" + ds + "/" + hiveTbName + "_" + bean.getOffset + "_" + bean.getTotal4Once

      val sbArray: Array[StringBuilder] = new Array[StringBuilder](len)
      for (i <- Range(0, len)) {
        sbArray.update(i, new StringBuilder(""))
      }
      rdd.map { r =>
        {
          val m = scala.collection.mutable.Map[Int, String]()
          for (i <- Range(0, len)) {
            val cv = (i, r.get(r.fieldIndex(coString.apply(i))).asInstanceOf[String])
            m += cv
          }
          m
        }
      }.collect().foreach(x => {
        for (i <- Range(0, len)) {
          val sb = sbArray.apply(i)
          sb.append("'").append(x.apply(i).replaceAll("'", "")).append("',")
        }
      })
      val lll: java.util.List[String] = new ArrayList[String]()
      for (i <- Range(0, len)) {
        val s3 = sbArray.apply(i)
        if (StringUtils.isNotBlank(s3)) {
          val idz = java.lang.Long.toString(Math.abs(UUID.randomUUID().getMostSignificantBits), 36)
          val z: String = idz + "\t" + s3.substring(0, s3.length - 1) +
            "\t" + dateDel + " 00:00:00" +
            "\t" + (bean.getOffset + "_" + bean.getTotal4Once)
          lll.add(z)
          smap.put(coString.apply(i) + "_" + idz, s3.substring(0, s3.length - 1))
        }
      }
      if (lll.size > 0 && dataType)
        sc.parallelize(lll.toArray()).repartition(1).saveAsTextFile(delFileName)
      if (!isTest) {
        val destFileName = url + "/tmp/mysqlGen/" + hiveSchema + "del/" + "omsdeldetail" + "/" + ds + "/" + hiveTbName + "_" + bean.getOffset + "_" + bean.getTotal4Once + ".txt"
        mysql.mergeHdfsFile(hdfs, destFileName, delFileName, hdfsUrl)
      }
      
      if(hdfs!=null)
        hdfs.close()
      smap
    }
  }

   def saveDelSql2Hdfs(appName: String, mode: String, hiveSchema: String, hiveTbName: String, hdfsUrl: String, strArray: Array[String], ds: String, pgNum: String, sc: SparkContext, isTest: Boolean) {
        val url = if (isTest) "" else hdfsUrl
        val mysql=new MysqlSpark()
        val delFileName = url + "/tmp/mysqlhive/" + hiveSchema + "del/" + "omsdel" + "/" + ds + "/" + hiveTbName + "_" ++ pgNum
        sc.parallelize(strArray).repartition(1).saveAsTextFile(delFileName)
        if(!isTest){
            val destFileName = url + "/tmp/mysqlGen/"  + hiveSchema + "del/" + "omsdel" + "/" + ds + "/" + hiveTbName + "_" ++ pgNum+".txt"
            val hdfs=new SparkHdfsUtil().getFileSystem(hdfsUrl)
            mysql.mergeHdfsFile(hdfs, destFileName, delFileName, hdfsUrl)
            hdfs.close()
          }
  }
   
  def getRDDAndSave2Hdfs(sc: SparkContext, sqlStr: String, hdfs: String, tableName: String, schema: String) {
    val d = HiveContextUtil.getRDD(sc, sqlStr)
    d.rdd.map { r =>
      {
        val s: StringBuilder = new StringBuilder("")
        for (j <- Range(0, r.length)) {
          var o = r.get(j)
          if (o == null)
            o = "\\N";
          if (o.isInstanceOf[String]) {
            if ("null".equals(o.asInstanceOf[String].toLowerCase()))
              o = "\\N";
            o = (o.asInstanceOf[String]).replaceAll("\n", " ");
            o = (o.asInstanceOf[String]).replaceAll("\t", " ");
          }
          s.append(o);
          if (j < r.length - 1)
            s.append("\t")
        }
        s
      }
    }.saveAsTextFile(hdfs + "/tmp/mysqlExport/" + schema + "/" + tableName + ".txt")
  }
}