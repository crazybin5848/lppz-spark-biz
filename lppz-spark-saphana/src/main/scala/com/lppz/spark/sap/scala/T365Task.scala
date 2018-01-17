package com.lppz.spark.sap.scala;

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
import com.lppz.spark.sap.bean.Hive2DbmsBean
import com.lppz.spark.scala.HiveContextUtil
import com.lppz.spark.scala.SparkHdfsUtil
import com.lppz.spark.sap.handler.HaNaJdbcHandler
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import com.lppz.spark.util.SparkYamlUtils
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

object T365Task extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName) 
  
  
  def main(args : Array[String])  = {
  
      val sc = new MysqlSpark().buildSc("T365Task", args(1))
      val bclass = new Hive2DbmsBean().getClass()
      val bean = SparkYamlUtils.loadYaml(args(0), false,bclass)
      HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
      HiveContextUtil.exec(sc, "truncate table tmpparquet.TAG_YEAR ")
   
      val calen = Calendar.getInstance()
      calen.add(Calendar.YEAR,-1)
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val timestr = sdf.format(new Date().getTime())
      val yearsago = sdf.format(calen.getTime())

      HiveContextUtil.exec(sc,"cache table mater_cache as select * from utags.ZMATERIAL");
      
               
      val sql ="insert overwrite table tmpparquet.TAG_YEAR                                                          "+
               "select  t.ZBPARTNE3 as ZBPARTNE3,                                                   "+
               "        CASE  WHEN t.ZGMZE >= 2500  THEN '10'                                       "+
               "              WHEN t.ZGMZE < 2500 AND t.ZGMZE >= 800 THEN '20'                      "+
               "              ELSE '30' END                                                         "+
               "          AS ZCX_GMZE                                                               "+
               " from                                                                               "+
               "(select u.ZBPARTNE3  as ZBPARTNE3 ,sum(u.ZARAMT)  as  ZGMZE                         "+
               "   from (SELECT  t1.calday as calday ,t1.ZBPARTNE3 as ZBPARTNE3,                    "+
               "                 t1.ZARAMT as  ZARAMT                                               "+
               "           from  parquetustag.AZPWSD00800 t1                                                     "+
               "      left join  mater_cache t2                                                      "+
               "             on ( t1.ZMATERIAL  = t2.ZMATERIAL)                                     "+
               "          where t2.ZKTGRM in ('00001','00004')                                      "+
               "            and t1.ZBPARTNE3 <> 'NULL' and  length(t1.ZBPARTNE3) <> 0 and t1.ZBPARTNE3 is not null                                              "+
               "            and t1.RPATTC in ('Z001','Z002','Z011','Z012','Z013','Z014')            "+
               "union                                                                               "+
               "select t1.calday as calday ,t1.ZBPARTNE3 as ZBPARTNE3, t1.ZARAMT as  ZARAMT         "+
               "  from parquetustag.AZPWSD00900 t1                                                               "+
               "left join mater_cache t2                                                             "+
               "on (t1.ZMATERIAL  = t2.ZMATERIAL)                                                   "+
               "where t2.ZKTGRM in ('00001','00004')                                                "+
               "  and t1.ZODTYPE  in ('Z001','Z002','Z011','Z012','Z013','Z014')                    "+
               "  and t1.ZBPARTNE3 <> 'NULL' and  length(t1.ZBPARTNE3) <> 0 and t1.ZBPARTNE3 is not null                                                       "+
               "union                                                                               "+
               "select t1.calday as calday ,t1.ZBPARTNE3 as ZBPARTNE3, t1.ZARAMT as  ZARAMT         "+
               "  from parquetustag.AZPWSD00600 t1                                                               "+
               "left join  mater_cache t2                                                            "+
               "on (t1.ZMATERIAL  = t2.ZMATERIAL)                                                   "+
               "where t1.ZODSTATU  not in ('CANCELLED','INVALID')                                                 "+
               "and t1.zodtype<>'replenishOrder' and t1.zeclpsbm ='0'                                     "+
               "and t2.matl_type NOT IN ('Z070','Z080','Z090')"+
               "and t1.ZBPARTNE3 <> 'NULL' and  length(t1.ZBPARTNE3) <> 0 and t1.ZBPARTNE3 is not null                                                          "+
               ") u                                                                                 "+
               "where  u.calday >= %s and u.calday <= %s                                        "+
               "group by u.ZBPARTNE3                                                                "+
               ") t"
               HiveContextUtil.exec(sc,sql.format(yearsago,timestr)) 

               HiveContextUtil.exec(sc,"uncache table mater_cache")
  }
                  
}