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
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date

object TGWCZTask extends Serializable {
  @transient lazy val LOG = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {

    val sc = new MysqlSpark().buildSc("TGWCZTask", args(1))
    val bclass = new Hive2DbmsBean().getClass()
    val bean = SparkYamlUtils.loadYaml(args(0), false, bclass)
    HiveContextUtil.exec(sc, "use " + bean.getHiveSchema)
    HiveContextUtil.exec(sc, "truncate table tmpparquet.TAG_GWCZ")

    HiveContextUtil.exec(sc, "cache table mater_cache as select * from utags.ZMATERIAL")

    val sql = """insert overwrite table tmpparquet.TAG_GWCZ                                                                                         
                select t.ZBPARTNE3,                                                                                             
                 case when t.calday > '19000101' then t.calday else  '19000101' end AS ZLAST_GW ,                               
                 CASE WHEN t.CZDATE <> '' THEN 'Y' ELSE 'N' END AS ZSFCZ,t.ZHYSRY as ZHY_SRY,t.ZHYXZ as ZHY_XZ                  
                 from (                                                                                                         
                select T2.ZBPARTNE3 ,MAX(T1.max_day) as calday,max(T3.CALDAY) AS CZDATE,T2.ZHYSRY,T2.ZHYXZ                      
                  from parquetustag.PZBPARTNE3  T2                                                                             
                 left join (select tmp.ZBPARTNE3,max(tmp.CALDAY) as max_day                                                       
                from (select ZBPARTNE3,max(calday) as calday from  parquetustag.AZPWSD00800 a
               inner join mater_cache b on a.zmaterial = b.ZMATERIAL 
            where (a.ZCFLAG = 'NULL' or  length(a.ZCFLAG)=0 or a.ZCFLAG is null)
             AND (a.ZBPARTNE3 <> 'NULL' or  length(a.ZBPARTNE3) <> 0 or a.ZBPARTNE3 is not null)
              AND b.ZKTGRM IN ('00001','00004')
              AND a.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014')
              group by ZBPARTNE3
              union all
              select ZBPARTNE3,max(calday) as calday from  parquetustag.AZPWSD00900 a
               inner join mater_cache b on a.zmaterial = b.ZMATERIAL 
            where (a.ZCFLAG = 'NULL' or  length(a.ZCFLAG)=0 or a.ZCFLAG is null)
             AND (a.ZBPARTNE3 <> 'NULL' or  length(a.ZBPARTNE3) <> 0 or a.ZBPARTNE3 is not null)
              AND b.ZKTGRM IN ('00001','00004')
              AND a.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014')
              group by ZBPARTNE3
              union all
              select ZBPARTNE3,max(calday) as calday from  parquetustag.AZPWSD00600 a
               inner join mater_cache b on a.zmaterial = b.ZMATERIAL 
            where (a.ZCFLAG = 'NULL' or  length(a.ZCFLAG)=0 or a.ZCFLAG is null)
             AND (a.ZBPARTNE3 <> 'NULL' or  length(a.ZBPARTNE3) <> 0 or a.ZBPARTNE3 is not null)
              AND b.ZKTGRM IN ('00001','00004')
              AND a.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014')
              group by ZBPARTNE3
             ) tmp GROUP BY tmp.ZBPARTNE3) T1                                                                                      
                 ON T2.ZBPARTNE3 = T1.ZBPARTNE3                                                                                 
                 LEFT JOIN parquetustag.AZPWHY00400 T3                                                                         
                 ON T2.ZBPARTNE3 = T3.ZBPARTNE3                                                                                 
                 GROUP BY T2.ZBPARTNE3,T2.ZHYSRY,T2.ZHYXZ) t """

    HiveContextUtil.exec(sc, sql)

    HiveContextUtil.exec(sc, "uncache table mater_cache")

  }

}