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

object T90Task extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName) 
  
  
  def main(args : Array[String])  = {
  
      val sc = new MysqlSpark().buildSc("T90Task", args(1))
      val bclass = new Hive2DbmsBean().getClass()
      val bean = SparkYamlUtils.loadYaml(args(0), false,bclass)
      val calen3 = Calendar.getInstance()
          calen3.add(Calendar.MONTH,-3)
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val timestr = sdf.format(new Date().getTime())
      val threemonthago = sdf.format(calen3.getTime())
      HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
      HiveContextUtil.exec(sc, "truncate table  tmpparquet.TAG_90")
      
      //drop临时表
      
      HiveContextUtil.exec(sc, "truncate table  tmpparquet.t_90_qudao")
      HiveContextUtil.exec(sc, "truncate table  tmpparquet.t_90_kouwei")
      HiveContextUtil.exec(sc, "truncate table  tmpparquet.t_90_pinlei")
      HiveContextUtil.exec(sc, "truncate table  tmpparquet.t_90_danpin")
      HiveContextUtil.exec(sc, "truncate table  tmpparquet.t_90_riqi")
      HiveContextUtil.exec(sc, "truncate table  tmpparquet.t_90_goumai")
 
      
      //渠道
      val sql ="insert overwrite table tmpparquet.t_90_qudao                                                                      "+
               "select t.BPARTNER,max(t.ZCX_XFQD1) as ZCX_XFQD1,max(t.ZCX_XFQD2) as ZCX_XFQD2,                         "+
               "       max(t.ZCX_XFQD3) as ZCX_XFQD3,max(t.ZCX_XFQD) as ZCX_XFQD                                       "+
               "from (                                                                                                 "+
               "select t3.BPARTNER,                                                                                    "+
               "       case when t3.RANK = 1 then t3.ZXFQD else null end as ZCX_XFQD1,                                 "+
               "       case when t3.RANK = 2 then t3.ZXFQD else null end as ZCX_XFQD2,                                 "+
               "       case when t3.RANK = 3 then t3.ZXFQD else null end as ZCX_XFQD3,                                 "+
               "       case when t3.RANK1 =1 and t3.PERSENT > 0.5 THEN t3.ZXFQD ELSE '999' END as ZCX_XFQD             "+
               "from (                                                                                                 "+
               "SELECT t2.BPARTNER,t2.ZXFQD,t2.PERSENT,t2.RANK,                                                        "+
               "       RANK() OVER(PARTITION BY t2.BPARTNER ORDER BY t2.PERSENT DESC) as RANK1                         "+
               "FROM                                                                                                   "+
               "(                                                                                                      "+
               "SELECT t1.BPARTNER,t1.ZXFQD,t1.JSQ,t1.JSQ/SUM(t1.JSQ) OVER(PARTITION BY t1.BPARTNER) AS PERSENT,       "+
               "       RANK()OVER(PARTITION BY t1.BPARTNER ORDER BY t1.JSQ DESC) AS RANK                               "+
               " FROM                                                                                                  "+
               "(select x.BPARTNER,x.ZXFQD,count(distinct x.ZORDER) AS JSQ                                             "+
               "from (select t.CALDAY as CALDAY,                                                                       "+
               "             t.ZORDER as ZORDER,                                                                       "+
               "             t.ZBPARTNE3 as BPARTNER,                                                                  "+
               "             t.ZMM_SYB as SYB,                                                                         "+
               "             t.ZMATERIAL as SKU,                                                                       "+
               "             t.ZXSQD as ZXFQD,                                                                         "+
               "             t.ZSALEXD as ZXDPH,                                                                       "+
               "             t.ZTIMERAG as ZSDPH,                                                                      "+
               "             t.ZFGZR as ZGZRPH,                                                                        "+
               "             t.ZFJJR as ZJJRPH,                                                                        "+
               "             t.ZMM_SPKW as ZKWPH,                                                                      "+
               "             t.ZWLDL as ZPLPH,                                                                         "+
               "             t.ZARAMT as ZJE,                                                                          "+
               "             t.ZCK05 as JSQ                                                                            "+
               " from tmpparquet.t_ZCMSD002 t                                                                                     "+
               " where t.CALDAY >= %s and  t.CALDAY <= %s                                                              "+
               " AND t.ZCFLAG = 'NULL'                                                                                 "+
               " AND t.ZBPARTNE3 <> 'NULL'                                                                             "+
               " AND t.ZKTGRM IN ('00001','00004')                                                                     "+
               " AND t.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014') ) x                                      "+
               "group by x.BPARTNER,x.ZXFQD) t1                                                                        "+
               "ORDER BY t1.BPARTNER ) t2                                                                              "+
               ") t3                                                                                                   "+
               " ) t                                                                                                   "+
               " group by t.BPARTNER                                                                                   "
      HiveContextUtil.exec(sc,sql.format(threemonthago,timestr))
     
      
      //口味
      val sql1 ="insert overwrite table tmpparquet.t_90_kouwei                                                                      "+
               "select t.BPARTNER,max(t.ZCX_KWPH1) as ZCX_KWPH1,max(t.ZCX_KWPH2) as ZCX_KWPH2,                         "+
               "       max(t.ZCX_KWPH3) as ZCX_KWPH3,max(t.ZCX_KWPH) as ZCX_KWPH                                       "+
               "from (                                                                                                 "+
               "select t3.BPARTNER,                                                                                    "+
               "       case when t3.RANK = 1 then t3.ZKWPH else null end as ZCX_KWPH1,                                 "+
               "       case when t3.RANK = 2 then t3.ZKWPH else null end as ZCX_KWPH2,                                 "+
               "       case when t3.RANK = 3 then t3.ZKWPH else null end as ZCX_KWPH3,                                 "+
               "       case when t3.RANK1 =1 and t3.PERSENT > 0.5 THEN t3.ZKWPH ELSE '999' END as ZCX_KWPH             "+
               "from (                                                                                                 "+
               "SELECT t2.BPARTNER,t2.ZKWPH,t2.PERSENT,t2.RANK,                                                        "+
               "       RANK() OVER(PARTITION BY t2.BPARTNER ORDER BY t2.PERSENT DESC) as RANK1                         "+
               "FROM                                                                                                   "+
               "(                                                                                                      "+
               "SELECT t1.BPARTNER,t1.ZKWPH,t1.JSQ,t1.JSQ/SUM(t1.JSQ) OVER(PARTITION BY t1.BPARTNER) AS PERSENT,       "+
               "       RANK()OVER(PARTITION BY t1.BPARTNER ORDER BY t1.JSQ DESC) AS RANK                               "+
               " FROM                                                                                                  "+
               "(select x.BPARTNER,x.ZKWPH,count(distinct x.ZORDER) AS JSQ                                             "+
               "from (select t.CALDAY as CALDAY,                                                                       "+
               "             t.ZORDER as ZORDER,                                                                       "+
               "             t.ZBPARTNE3 as BPARTNER,                                                                  "+
               "             t.ZMM_SYB as SYB,                                                                         "+
               "             t.ZMATERIAL as SKU,                                                                       "+
               "             t.ZXSQD as ZXFQD,                                                                         "+
               "             t.ZSALEXD as ZXDPH,                                                                       "+
               "             t.ZTIMERAG as ZSDPH,                                                                      "+
               "             t.ZFGZR as ZGZRPH,                                                                        "+
               "             t.ZFJJR as ZJJRPH,                                                                        "+
               "             t.ZMM_SPKW as ZKWPH,                                                                      "+
               "             t.ZWLDL as ZPLPH,                                                                         "+
               "             t.ZARAMT as ZJE,                                                                          "+
               "             t.ZCK05 as JSQ                                                                            "+
               " from tmpparquet.t_ZCMSD002 t                                                                                     "+
               " where t.CALDAY >= %s and  t.CALDAY <= %s                                                              "+
               " AND t.ZCFLAG = 'NULL'                                                                                 "+
               " AND t.ZBPARTNE3 <> 'NULL'                                                                             "+
               " AND t.ZKTGRM IN ('00001','00004')                                                                     "+
               " AND t.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014') ) x                                      "+
               "group by x.BPARTNER,x.ZKWPH) t1                                                                        "+
               "ORDER BY t1.BPARTNER ) t2                                                                              "+
               ") t3                                                                                                   "+
               " ) t                                                                                                   "+
               " group by t.BPARTNER                                                                                   "
      HiveContextUtil.exec(sc,sql1.format(threemonthago,timestr))
     
      
      //品类
      val sql2 ="insert overwrite table tmpparquet.t_90_pinlei                                                                      "+
               "select t.BPARTNER,max(t.ZCX_PLPH1) as ZCX_PLPH1,max(t.ZCX_PLPH2) as ZCX_PLPH2,                         "+
               "       max(t.ZCX_PLPH3) as ZCX_PLPH3,max(t.ZCX_PLPH) as ZCX_PLPH                                       "+
               "from (                                                                                                 "+
               "select t3.BPARTNER,                                                                                    "+
               "       case when t3.RANK = 1 then substr(t3.ZPLPH,-2,2) else null end as ZCX_PLPH1,                                 "+
               "       case when t3.RANK = 2 then substr(t3.ZPLPH,-2,2) else null end as ZCX_PLPH2,                                 "+
               "       case when t3.RANK = 3 then substr(t3.ZPLPH,-2,2) else null end as ZCX_PLPH3,                                 "+
               "       case when t3.RANK1 =1 and t3.PERSENT > 0.5 THEN t3.ZPLPH ELSE '999' END as ZCX_PLPH             "+
               "from (                                                                                                 "+
               "SELECT t2.BPARTNER,t2.ZPLPH,t2.PERSENT,t2.RANK,                                                        "+
               "       RANK() OVER(PARTITION BY t2.BPARTNER ORDER BY t2.PERSENT DESC) as RANK1                         "+
               "FROM                                                                                                   "+
               "(                                                                                                      "+
               "SELECT t1.BPARTNER,t1.ZPLPH,t1.JSQ,t1.JSQ/SUM(t1.JSQ) OVER(PARTITION BY t1.BPARTNER) AS PERSENT,       "+
               "       RANK()OVER(PARTITION BY t1.BPARTNER ORDER BY t1.JSQ DESC) AS RANK                               "+
               " FROM                                                                                                  "+
               "(select x.BPARTNER,x.ZPLPH,count(distinct x.ZORDER) AS JSQ                                             "+
               "from (select t.CALDAY as CALDAY,                                                                       "+
               "             t.ZORDER as ZORDER,                                                                       "+
               "             t.ZBPARTNE3 as BPARTNER,                                                                  "+
               "             t.ZMM_SYB as SYB,                                                                         "+
               "             t.ZMATERIAL as SKU,                                                                       "+
               "             t.ZXSQD as ZXFQD,                                                                         "+
               "             t.ZSALEXD as ZXDPH,                                                                       "+
               "             t.ZTIMERAG as ZSDPH,                                                                      "+
               "             t.ZFGZR as ZGZRPH,                                                                        "+
               "             t.ZFJJR as ZJJRPH,                                                                        "+
               "             t.ZMM_SPKW as ZKWPH,                                                                      "+
               "             t.ZWLDL as ZPLPH,                                                                         "+
               "             t.ZARAMT as ZJE,                                                                          "+
               "             t.ZCK05 as JSQ                                                                            "+
               " from tmpparquet.t_ZCMSD002 t                                                                                     "+
               " where t.CALDAY >= %s and  t.CALDAY <= %s                                                              "+
               " AND t.ZCFLAG = 'NULL'                                                                                 "+
               " AND t.ZBPARTNE3 <> 'NULL'                                                                             "+
               " AND t.ZKTGRM IN ('00001','00004')                                                                     "+
               " AND t.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014') ) x                                      "+
               "group by x.BPARTNER,x.ZPLPH) t1                                                                        "+
               "ORDER BY t1.BPARTNER ) t2                                                                              "+
               ") t3                                                                                                   "+
               " ) t                                                                                                   "+
               " group by t.BPARTNER                                                                                   "
      
      HiveContextUtil.exec(sc,sql2.format(threemonthago,timestr))
 
      //单品
      val sql3 ="insert overwrite table tmpparquet.t_90_danpin                                                                      "+
               "select t.BPARTNER,max(t.ZCX_DPPH1) as ZCX_DPPH1,max(t.ZCX_DPPH2) as ZCX_DPPH2,                         "+
               "       max(t.ZCX_DPPH3) as ZCX_DPPH3,max(t.ZCX_DPPH) as ZCX_DPPH                                       "+
               "from (                                                                                                 "+
               "select t3.BPARTNER,                                                                                    "+
               "       case when t3.RANK = 1 then t3.ZDPPH else null end as ZCX_DPPH1,                                 "+
               "       case when t3.RANK = 2 then t3.ZDPPH else null end as ZCX_DPPH2,                                 "+
               "       case when t3.RANK = 3 then t3.ZDPPH else null end as ZCX_DPPH3,                                 "+
               "       case when t3.RANK1 =1 and t3.PERSENT > 0.5 THEN t3.ZDPPH ELSE '999' END as ZCX_DPPH             "+
               "from (                                                                                                 "+
               "SELECT t2.BPARTNER,t2.ZDPPH,t2.PERSENT,t2.RANK,                                                        "+
               "       RANK() OVER(PARTITION BY t2.BPARTNER ORDER BY t2.PERSENT DESC) as RANK1                         "+
               "FROM                                                                                                   "+
               "(                                                                                                      "+
               "SELECT t1.BPARTNER,t1.ZDPPH,t1.JSQ,t1.JSQ/SUM(t1.JSQ) OVER(PARTITION BY t1.BPARTNER) AS PERSENT,       "+
               "       RANK()OVER(PARTITION BY t1.BPARTNER ORDER BY t1.JSQ DESC) AS RANK                               "+
               " FROM                                                                                                  "+
               "(select x.BPARTNER,x.SKU AS ZDPPH,count(distinct x.ZORDER) AS JSQ                                             "+
               "from (select t.CALDAY as CALDAY,                                                                       "+
               "             t.ZORDER as ZORDER,                                                                       "+
               "             t.ZBPARTNE3 as BPARTNER,                                                                  "+
               "             t.ZMM_SYB as SYB,                                                                         "+
               "             t.ZMATERIAL as SKU,                                                                       "+
               "             t.ZXSQD as ZXFQD,                                                                         "+
               "             t.ZSALEXD as ZXDPH,                                                                       "+
               "             t.ZTIMERAG as ZSDPH,                                                                      "+
               "             t.ZFGZR as ZGZRPH,                                                                        "+
               "             t.ZFJJR as ZJJRPH,                                                                        "+
               "             t.ZMM_SPKW as ZKWPH,                                                                      "+
               "             t.ZWLDL as ZPLPH,                                                                         "+
               "             t.ZARAMT as ZJE,                                                                          "+
               "             t.ZCK05 as JSQ                                                                            "+
               " from tmpparquet.t_ZCMSD002 t                                                                                     "+
               " where t.CALDAY >= %s and  t.CALDAY <= %s                                                              "+
               " AND t.ZCFLAG = 'NULL'                                                                                 "+
               " AND t.ZBPARTNE3 <> 'NULL'                                                                             "+
               " AND t.ZKTGRM IN ('00001','00004')                                                                     "+
               " AND t.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014') ) x                                      "+
               "group by x.BPARTNER,x.SKU) t1                                                                        "+
               "ORDER BY t1.BPARTNER ) t2                                                                              "+
               ") t3                                                                                                   "+
               " ) t                                                                                                   "+
               " group by t.BPARTNER                                                                                   "
               
      HiveContextUtil.exec(sc,sql3.format(threemonthago,timestr))
      
      
      val sql4 ="insert overwrite table tmpparquet.t_90_riqi                                                                                                  "+
               "select t.BPARTNER,max(t.ZCX_XDPH) as ZCX_XDPH,max(t.ZCX_JJR) as ZCX_JJR,                                                    "+
               "       max(t.ZCX_SDPH) as ZCX_SDPH,max(t.ZCX_ZMPH) as ZCX_ZMPH                                                              "+
               "from (SELECT t3.BPARTNER,CASE WHEN t3.PERSENT > 0.5 THEN t3.ZXDPH ELSE '40' END as ZCX_XDPH,                                "+
               "      null as ZCX_JJR,null as ZCX_SDPH,null as ZCX_ZMPH                                                                     "+
               "FROM (SELECT t2.BPARTNER,t2.ZXDPH,t2.PERSENT,RANK()OVER(PARTITION BY t2.BPARTNER ORDER BY t2.PERSENT DESC) AS RANK          "+
               "FROM (SELECT t1.BPARTNER,t1.ZXDPH,t1.JSQ/SUM(t1.JSQ) OVER(PARTITION BY t1.BPARTNER) AS PERSENT                              "+
               " FROM (select t.BPARTNER,t.ZXDPH,count(distinct t.ZORDER) AS JSQ                                                            "+
               "from (select t.CALDAY as CALDAY,t.ZORDER as ZORDER,t.ZBPARTNE3 as BPARTNER,t.ZMM_SYB as SYB,                                 "+
               "       t.ZMATERIAL as SKU,t.ZXSQD as ZXFQD,t.ZSALEXD as ZXDPH,t.ZTIMERAG as ZSDPH,t.ZFGZR as ZGZRPH,                        "+
               "       t.ZFJJR as ZJJRPH,t.ZMM_SPKW as ZKWPH,t.ZWLDL as ZPLPH,t.ZARAMT as ZJE,t.ZCK05 as JSQ                                 "+
               " from tmpparquet.t_ZCMSD002 t                                                                                                          "+
               " where t.CALDAY >= %s and  t.CALDAY <= %s  "+
               " AND t.ZCFLAG = 'NULL' AND t.ZBPARTNE3 != 'NULL'                                                                            "+
               " AND t.ZKTGRM IN ('00001','00004')                                                                                          "+
               " AND t.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014') ) t                                                           "+
               "group by t.BPARTNER,t.ZXDPH) t1                                                                                             "+
               " ORDER BY t1.BPARTNER                                                                                                       "+
               " ) t2 ) t3 WHERE t3.RANK = 1                                                                                                "+
               "union all                                                                                                                   "+
               "SELECT t3.BPARTNER,null as ZCX_XDPH,                                                                                        "+
               "       CASE WHEN t3.PERSENT > 0.5 THEN 'Y' ELSE 'N' END as ZCX_JJR,null as ZCX_SDPH,null as ZCX_ZMPH                        "+
               "FROM (SELECT t2.BPARTNER,t2.ZJJRPH,t2.PERSENT,RANK() OVER(PARTITION BY t2.BPARTNER ORDER BY t2.PERSENT DESC) AS RANK        "+
               "FROM (SELECT t1.BPARTNER,t1.ZJJRPH,t1.JSQ/SUM(t1.JSQ) OVER(PARTITION BY BPARTNER) AS PERSENT                              "+
               " FROM (select t.BPARTNER,t.ZJJRPH,count(distinct t.ZORDER) AS JSQ                                                           "+
               "from (select t.CALDAY as CALDAY,t.ZORDER as ZORDER,t.ZBPARTNE3 as BPARTNER,t.ZMM_SYB as SYB,t.ZMATERIAL as SKU,              "+
               "       t.ZXSQD as ZXFQD,t.ZSALEXD as ZXDPH,t.ZTIMERAG as ZSDPH,t.ZFGZR as ZGZRPH,t.ZFJJR as ZJJRPH,                         "+
               "       t.ZMM_SPKW as ZKWPH,t.ZWLDL as ZPLPH,t.ZARAMT as ZJE,t.ZCK05 as JSQ                                                   "+
               " from tmpparquet.t_ZCMSD002 t                                                                                                          "+
               " where t.CALDAY >= %s and  t.CALDAY <= %s  "+
               " AND t.ZCFLAG = 'NULL' AND t.ZBPARTNE3 != 'NULL'                                                                            "+
               " AND t.ZKTGRM IN ('00001','00004')                                                                                          "+
               " AND t.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014') ) t                                                           "+
               "group by t.BPARTNER,t.ZJJRPH) t1                                                                                            "+
               "ORDER BY t1.BPARTNER                                                                                                        "+
               " ) t2 ) t3 WHERE t3.RANK = 1                                                                                                "+
               "union all                                                                                                                   "+
               "SELECT t3.BPARTNER,null as ZCX_XDPH,null as ZCX_JJR,                                                                        "+
               "       CASE WHEN t3.PERSENT > 0.5 THEN t3.ZSDPH ELSE '60' END as ZCX_SDPH,null as ZCX_ZMPH                                  "+
               "FROM (SELECT t2.BPARTNER,t2.ZSDPH,t2.PERSENT,RANK() OVER(PARTITION BY t2.BPARTNER ORDER BY t2.PERSENT DESC) AS RANK         "+
               "FROM (SELECT t1.BPARTNER,t1.ZSDPH,t1.JSQ/SUM(t1.JSQ) OVER(PARTITION BY t1.BPARTNER) AS PERSENT                              "+
               "FROM (select t.BPARTNER,                                                                                                    "+
               "      CASE WHEN t.ZSDPH < '07' THEN '10'                                                                                    "+
               "           WHEN t.ZSDPH >= '07' AND t.ZSDPH < '12' THEN '20'                                                                "+
               "           WHEN t.ZSDPH >= '12' AND t.ZSDPH < '17' THEN '30'                                                                "+
               "           WHEN  t.ZSDPH >= '17' AND t.ZSDPH < '20' THEN '40'                                                               "+
               "           WHEN  t.ZSDPH >= '20' AND t.ZSDPH < '24' THEN '50' end AS ZSDPH,                                                 "+
               "     count(distinct t.ZORDER) AS JSQ                                                                                        "+
               "from (select t.CALDAY as CALDAY,t.ZORDER as ZORDER,t.ZBPARTNE3 as BPARTNER,t.ZMM_SYB as SYB,                                 "+
               "       t.ZMATERIAL as SKU,t.ZXSQD as ZXFQD,t.ZSALEXD as ZXDPH,t.ZTIMERAG as ZSDPH,t.ZFGZR as ZGZRPH,                        "+
               "       t.ZFJJR as ZJJRPH,t.ZMM_SPKW as ZKWPH,t.ZWLDL as ZPLPH,t.ZARAMT as ZJE,t.ZCK05 as JSQ                                 "+
               " from tmpparquet.t_ZCMSD002 t                                                                                                          "+
               " where t.CALDAY >= %s and  t.CALDAY <= %s  "+
               " AND t.ZCFLAG = 'NULL' AND t.ZBPARTNE3 != 'NULL'                                                                            "+
               " AND t.ZKTGRM IN ('00001','00004')                                                                                          "+
               " AND t.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014') ) t                                                           "+
               "group by t.BPARTNER,t.ZSDPH) t1                                                                                             "+
               "ORDER BY t1.BPARTNER                                                                                                        "+
               " )t2 )t3 WHERE t3.RANK = 1                                                                                                  "+
               "union all                                                                                                                   "+
               "SELECT t3.BPARTNER,null as ZCX_XDPH,null as ZCX_JJR,null as ZCX_SDPH,                                                       "+
               "      CASE WHEN t3.PERSENT > 0.85 THEN t3.ZGZRPH ELSE '30' END as ZCX_ZMPH                                                  "+
               "FROM (SELECT t2.BPARTNER,t2.ZGZRPH,t2.PERSENT,RANK() OVER(PARTITION BY t2.BPARTNER ORDER BY t2.PERSENT DESC) AS RANK        "+
               "FROM (SELECT t1.BPARTNER,t1.ZGZRPH,t1.JSQ/SUM(t1.JSQ) OVER(PARTITION BY t1.BPARTNER) AS PERSENT                             "+
               " FROM (select t.BPARTNER,                                                                                                   "+
               "case when t.ZGZRPH = 'Y' THEN '10' when t.ZGZRPH = 'N' THEN '20' end AS ZGZRPH,                                             "+
               "count(distinct t.ZORDER) AS JSQ                                                                                             "+
               "from (select t.CALDAY as CALDAY,t.ZORDER as ZORDER,t.ZBPARTNE3 as BPARTNER,t.ZMM_SYB as SYB,t.ZMATERIAL as SKU,              "+
               "       t.ZXSQD as ZXFQD,t.ZSALEXD as ZXDPH,t.ZTIMERAG as ZSDPH,t.ZFGZR as ZGZRPH,t.ZFJJR as ZJJRPH,                         "+
               "       t.ZMM_SPKW as ZKWPH,t.ZWLDL as ZPLPH,t.ZARAMT as ZJE,t.ZCK05 as JSQ                                                   "+
               " from tmpparquet.t_ZCMSD002 t                                                                                                          "+
               " where t.CALDAY >= %s and  t.CALDAY <= %s   "+
               " AND t.ZCFLAG = 'NULL' AND t.ZBPARTNE3 != 'NULL'                                                                            "+
               " AND t.ZKTGRM IN ('00001','00004')                                                                                          "+
               " AND t.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014') ) t                                                           "+
               "group by t.BPARTNER,t.ZGZRPH)t1                                                                                             "+
               "ORDER BY t1.BPARTNER                                                                                                        "+
               " )t2 )t3 WHERE t3.RANK = 1                                                                                                  "+
               " ) t                                                                                                                        "+
               " group by t.BPARTNER                                                                                                        "
      HiveContextUtil.exec(sc,sql4.format(threemonthago,timestr,threemonthago,timestr,threemonthago,timestr,threemonthago,timestr))
     
      //购买
      val sql5 = "insert overwrite table tmpparquet.t_90_goumai                                                                                              "+                                                                                                                                                                     
               "SELECT  t2.BPARTNER,max(t2.ZCX_DBJE) as ZCX_DBJE,max(t2.ZCX_GWSC) as ZCX_GWSC,                                                       "+
               "        max(t2.ZCX_DBSKU) as ZCX_DBSKU,max(t2.ZCX_GMPC) as ZCX_GMPC,max(t2.ZCX_DBPL) as ZCX_DBPL                                     "+
               "FROM                                                                                                                                 "+
               "(                                                                                                                                    "+
               "SELECT t1.BPARTNER,CASE WHEN t1.ZKDJ >= 100 THEN '10' WHEN t1.ZKDJ < 100 AND t1.ZKDJ >= 68 THEN '20' ELSE '30' END AS ZCX_DBJE,      "+
               "null as ZCX_GWSC,CASE WHEN t1.ZDBSKU >= 10 THEN '10' WHEN t1.ZDBSKU < 10 AND t1.ZDBSKU >= 5 THEN '20' ELSE '30' END AS ZCX_DBSKU,    "+
               "CASE WHEN t1.ZGMPC/3 >= 3 THEN '10' WHEN t1.ZGMPC/3 < 3 AND t1.ZGMPC/3 >= 1 THEN '20' ELSE '30' END AS ZCX_GMPC,                     "+
               "null as ZCX_DBPL                                                                                                                     "+
               " FROM                                                                                                                                "+
               "(select x.BPARTNER,SUM(x.ZJE)/count(distinct x.ZORDER) AS ZKDJ,                                                                      "+
               "COUNT(x.SKU)/count(distinct x.ZORDER) AS ZDBSKU,                                                                                     "+
               "count(distinct x.ZORDER) AS ZGMPC                                                                                                    "+
               "from (select t.CALDAY as CALDAY,                                                                                                     "+
               "             t.ZORDER as ZORDER,                                                                                                     "+
               "             t.ZBPARTNE3 as BPARTNER,                                                                                                "+
               "             t.ZMM_SYB as SYB,                                                                                                       "+
               "             t.ZMATERIAL as SKU,                                                                                                     "+
               "             t.ZXSQD as ZXFQD,                                                                                                       "+
               "             t.ZSALEXD as ZXDPH,                                                                                                     "+
               "             t.ZTIMERAG as ZSDPH,                                                                                                    "+
               "             t.ZFGZR as ZGZRPH,                                                                                                      "+
               "             t.ZFJJR as ZJJRPH,                                                                                                      "+
               "             t.ZMM_SPKW as ZKWPH,                                                                                                    "+
               "             t.ZWLDL as ZPLPH,                                                                                                       "+
               "             t.ZARAMT as ZJE,                                                                                                        "+
               "             t.ZCK05 as JSQ                                                                                                          "+
               " from tmpparquet.t_ZCMSD002 t                                                                                                        "+
               " where t.CALDAY >= %s and  t.CALDAY <= %s                                                                                            "+
               " AND t.ZCFLAG = 'NULL'                                                                                                               "+
               " AND t.ZBPARTNE3 <> 'NULL'                                                                                                           "+
               " AND t.ZKTGRM IN ('00001','00004')                                                                                                   "+
               " AND t.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014') ) x                                                                    "+
               "group by x.BPARTNER) t1                                                                                                              "+
               "union all                                                                                                                            "+
               "SELECT t.ZBPARTNE3,null as ZCX_DBJE,                                                                                                 "+
               "CASE WHEN t.GWJG >= 7 AND t.GWJG < 15 THEN '20'                                                                                      "+
               "WHEN t.GWJG >=15 AND t.GWJG < 30 THEN '30'                                                                                           "+
               "WHEN t.GWJG >=30 AND t.GWJG < 45 THEN '40'                                                                                           "+
               "WHEN t.GWJG >=45 THEN '50'                                                                                                           "+
               "WHEN t.GWJG < 7  THEN '10'                                                                                                           "+
               "END AS ZCX_GWSC,null as ZCX_DBSKU,null as ZCX_GMPC,null as ZCX_DBPL                                                                  "+
               "FROM (                                                                                                                               "+
               "SELECT t.ZBPARTNE3,t.ZLAST_GW,                                                                                                       "+
               "datediff(TO_DATE(from_unixtime(unix_timestamp())),to_date(from_unixtime(unix_timestamp(t.ZLAST_GW, 'yyyyMMdd')))) AS GWJG            "+
               " FROM tmpparquet.TAG_GWCZ t  WHERE t.ZLAST_GW <> '19000101') t                                                                       "+
               " union all                                                                                                                           "+
               " select t.BPARTNER, null as ZCX_DBJE,null AS ZCX_GWSC,null as ZCX_DBSKU,null as ZCX_GMPC,                                            "+
               " CASE WHEN t.ZDBPL >= 5 THEN '10' WHEN t.ZDBPL < 5 AND t.ZDBPL >= 3 THEN '20' ELSE '30' END AS ZCX_DBPL                              "+
               " FROM (SELECT t1.BPARTNER,SUM(t1.GMPL)/COUNT(t1.ZORDER) AS ZDBPL                                                                     "+
               " FROM (select t.BPARTNER ,t.ZORDER,COUNT(DISTINCT(t.ZPLPH)) AS GMPL                                                                  "+
               " FROM (select t.CALDAY as CALDAY,                                                                                                    "+
               "              t.ZORDER as ZORDER,                                                                                                    "+
               "              t.ZBPARTNE3 as BPARTNER,                                                                                               "+
               "              t.ZMM_SYB as SYB,                                                                                                      "+
               "              t.ZMATERIAL as SKU,                                                                                                    "+
               "              t.ZXSQD as ZXFQD,                                                                                                      "+
               "              t.ZSALEXD as ZXDPH,                                                                                                    "+
               "              t.ZTIMERAG as ZSDPH,                                                                                                   "+
               "              t.ZFGZR as ZGZRPH,                                                                                                     "+
               "              t.ZFJJR as ZJJRPH,                                                                                                     "+
               "              t.ZMM_SPKW as ZKWPH,                                                                                                   "+
               "              t.ZWLDL as ZPLPH,                                                                                                      "+
               "              t.ZARAMT as ZJE,                                                                                                       "+
               "              t.ZCK05 as JSQ                                                                                                         "+
               "   from tmpparquet.t_ZCMSD002 t                                                                                                      "+
               "   where t.CALDAY >= %s and  t.CALDAY <= %s                                                                                          "+
               "   AND t.ZCFLAG = 'NULL'  AND t.ZBPARTNE3 <> 'NULL'                                                                                  "+
               "   AND t.ZKTGRM IN ('00001','00004')                                                                                                 "+
               "   AND t.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014') ) t                                                                  "+
               "   GROUP BY t.BPARTNER,t.ZORDER) t1                                                                                                  "+
               "   GROUP BY t1.BPARTNER) t                                                                                                           "+
               "    ) t2                                                                                                                             "+
               "    group by t2.BPARTNER                                                                                                           "                                             
      HiveContextUtil.exec(sc,sql5.format(threemonthago,timestr,threemonthago,timestr))
       
      
      //汇总
      val sql6 ="insert into tmpparquet.TAG_90                                                                                                     "+
               "select t.ZBPARTNE3,max(t.zcx_xfqd1) as zcx_xfqd1,max(t.zcx_xfqd2) as zcx_xfqd2,                                        "+
               "max(t.zcx_xfqd3) as zcx_xfqd3,max(t.zcx_xfqd) as zcx_xfqd,                                                             "+
               "max(t.zcx_kwph1) as zcx_kwph1,max(t.zcx_kwph2) as zcx_kwph2,max(t.zcx_kwph3) as zcx_kwph3,max(t.zcx_kwph) as zcx_kwph, "+
               "max(t.zcx_plph1) as zcx_plph1,max(t.zcx_plph2) as zcx_plph2,max(t.zcx_plph3) as zcx_plph3,max(t.zcx_plph) as zcx_plph, "+
               "max(t.zcx_dpph1) as zcx_dpph1,max(t.zcx_dpph2) as zcx_dpph2,max(t.zcx_dpph3) as zcx_dpph3,max(t.zcx_dpph) as zcx_dpph, "+
               "max(t.zcx_xdph) as zcx_xdph,max(t.zcx_jjr) as zcx_jjr,max(t.zcx_sdph) as zcx_sdph,max(t.zcx_zmph) as zcx_zmph,         "+
               "max(t.zcx_dbje) as zcx_dbje,max(t.zcx_gwsc) as zcx_gwsc,max(t.zcx_dbsku) as zcx_dbsku,                                 "+
               "max(t.zcx_gmpc) as zcx_gmpc,max(t.zcx_dbpl) as zcx_dbpl                                                                "+
               "from (select a.bpartner as ZBPARTNE3,a.zcx_xfqd1,a.zcx_xfqd2,a.zcx_xfqd3,a.zcx_xfqd,                                   "+
               "null as zcx_kwph1,null as zcx_kwph2,null as zcx_kwph3,null as zcx_kwph,                                                "+
               "null as zcx_plph1,null as zcx_plph2,null as zcx_plph3,null as zcx_plph,                                                "+
               "null as zcx_dpph1,null as zcx_dpph2,null as zcx_dpph3,null as zcx_dpph,                                                "+
               "null as zcx_xdph,null as zcx_jjr,null as zcx_sdph,null as zcx_zmph,                                                    "+
               "null as zcx_dbje,null as zcx_gwsc,null as zcx_dbsku,null as zcx_gmpc,null as zcx_dbpl                                  "+
               "from tmpparquet.t_90_qudao a                                                                                                      "+
               "union all                                                                                                              "+
               "select b.bpartner as ZBPARTNE3,null as zcx_xfqd1,null as zcx_xfqd2,null as zcx_xfqd3,null as zcx_xfqd,                 "+
               "b.zcx_kwph1,b.zcx_kwph2,b.zcx_kwph3,b.zcx_kwph,                                                                        "+
               "null as zcx_plph1,null as zcx_plph2,null as zcx_plph3,null as zcx_plph,                                                "+
               "null as zcx_dpph1,null as zcx_dpph2,null as zcx_dpph3,null as zcx_dpph,                                                "+
               "null as zcx_xdph,null as zcx_jjr,null as zcx_sdph,null as zcx_zmph,                                                    "+
               "null as zcx_dbje,null as zcx_gwsc,null as zcx_dbsku,null as zcx_gmpc,null as zcx_dbpl                                  "+
               "from tmpparquet.t_90_kouwei b                                                                                                     "+
               "union all                                                                                                              "+
               "select c.bpartner as ZBPARTNE3,null as zcx_xfqd1,null as zcx_xfqd2,null as zcx_xfqd3,null as zcx_xfqd,                 "+
               "null as zcx_kwph1,null as zcx_kwph2,null as zcx_kwph3,null as zcx_kwph,                                                "+
               "c.zcx_plph1,c.zcx_plph2,c.zcx_plph3,c.zcx_plph,                                                                        "+
               "null as zcx_dpph1,null as zcx_dpph2,null as zcx_dpph3,null as zcx_dpph,                                                "+
               "null as zcx_xdph,null as zcx_jjr,null as zcx_sdph,null as zcx_zmph,                                                    "+
               "null as zcx_dbje,null as zcx_gwsc,null as zcx_dbsku,null as zcx_gmpc,null as zcx_dbpl                                  "+
               "from tmpparquet.t_90_pinlei c                                                                                                     "+
               "union all                                                                                                              "+
               "select d.bpartner as ZBPARTNE3,null as zcx_xfqd1,null as zcx_xfqd2,null as zcx_xfqd3,null as zcx_xfqd,                 "+
               "null as zcx_kwph1,null as zcx_kwph2,null as zcx_kwph3,null as zcx_kwph,                                                "+
               "null as zcx_plph1,null as zcx_plph2,null as zcx_plph3,null as zcx_plph,                                                "+
               "d.zcx_dpph1,d.zcx_dpph2,d.zcx_dpph3,d.zcx_dpph,                                                                        "+
               "null as zcx_xdph,null as zcx_jjr,null as zcx_sdph,null as zcx_zmph,                                                    "+
               "null as zcx_dbje,null as zcx_gwsc,null as zcx_dbsku,null as zcx_gmpc,null as zcx_dbpl                                  "+
               "from tmpparquet.t_90_danpin d                                                                                                     "+
               "union all                                                                                                              "+
               "select e.bpartner as ZBPARTNE3,null as zcx_xfqd1,null as zcx_xfqd2,null as zcx_xfqd3,null as zcx_xfqd,                 "+
               "null as zcx_kwph1,null as zcx_kwph2,null as zcx_kwph3,null as zcx_kwph,                                                "+
               "null as zcx_plph1,null as zcx_plph2,null as zcx_plph3,null as zcx_plph,                                                "+
               "null as zcx_dpph1,null as zcx_dpph2,null as zcx_dpph3,null as zcx_dpph,                                                "+
               "e.zcx_xdph,e.zcx_jjr,e.zcx_sdph,e.zcx_zmph,                                                                            "+
               "null as zcx_dbje,null as zcx_gwsc,null as zcx_dbsku,null as zcx_gmpc,null as zcx_dbpl                                  "+
               "from tmpparquet.t_90_riqi e                                                                                                       "+
               "union all                                                                                                              "+
               "select f.bpartner as ZBPARTNE3,null as zcx_xfqd1,null as zcx_xfqd2,null as zcx_xfqd3,null as zcx_xfqd,                 "+
               "null as zcx_kwph1,null as zcx_kwph2,null as zcx_kwph3,null as zcx_kwph,                                                "+
               "null as zcx_plph1,null as zcx_plph2,null as zcx_plph3,null as zcx_plph,                                                "+
               "null as zcx_dpph1,null as zcx_dpph2,null as zcx_dpph3,null as zcx_dpph,                                                "+
               "null as zcx_xdph,null as zcx_jjr,null as zcx_sdph,null as zcx_zmph,                                                    "+
               "f.zcx_dbje,f.zcx_gwsc,f.zcx_dbsku,f.zcx_gmpc,f.zcx_dbpl                                                                "+
               "from tmpparquet.t_90_goumai f                                                                                                     "+
               ") t                                                                                                                    "+
               "group by t.ZBPARTNE3                                                                                                   "
      HiveContextUtil.exec(sc,sql6)
      
  }
                  
}