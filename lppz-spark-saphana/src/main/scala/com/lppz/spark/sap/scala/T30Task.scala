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

object T30Task extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName) 
  
  
  def main(args : Array[String])  = {
  
      val sc = new MysqlSpark().buildSc("T30Task", args(1))
      val bclass = new Hive2DbmsBean().getClass()
      val bean = SparkYamlUtils.loadYaml(args(0), false,bclass)
      val calen1 = Calendar.getInstance()
          calen1.add(Calendar.MONTH,-1)
      val calen3 = Calendar.getInstance()
          calen3.add(Calendar.MONTH,-3)
      val calen6 = Calendar.getInstance()
          calen6.add(Calendar.MONTH,-6)
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val timestr = sdf.format(new Date().getTime())
      val monthago = sdf.format(calen1.getTime())
      val threemonthago = sdf.format(calen3.getTime())
      val sixmonthago = sdf.format(calen6.getTime())
      
      HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
      
      //创建zhy_zcd指标表
      HiveContextUtil.exec(sc, "truncate table tmpparquet.t_30_zhyzcd")
      
      val sql = "insert overwrite table tmpparquet.t_30_zhyzcd                                                                                             "+
                "SELECT t.BPARTNER,CASE WHEN t.JSQ >= 3 THEN '20' ELSE '30' END AS ZHY_ZCD,unix_timestamp() as create_time                 "+
                "FROM (                                                                                                                    "+
                "SELECT t4.BPARTNER,COUNT(DISTINCT(t4.CALDAY)) AS JSQ                                                                      "+
                "FROM (select t1.CALDAY,t1.ZORDER,t1.ZBPARTNE3 as BPARTNER,t1.zmm_syb as SYB,t1.ZMATERIAL as SKU,t1.ZXSQD as ZXFQD,         "+
                "             t1.ZSALEXD as ZXDPH,t1.ZTIMERAG as ZSDPH,t1.ZFGZR as ZGZRPH,t1.ZFJJR as ZJJRPH,t1.zmm_szb as ZKWPH,           "+
                "             t1.ZWLDL as ZPLPH,t1.ZARAMT as ZJE,t1.ZCK05 as JSQ                                                           "+
                " from tmpparquet.t_ZCMSD002 T1                                                                                                       "+
                " where t1.calday >= %s and t1.calday <= %s "+
                " AND t1.ZCFLAG = 'NULL' and  length(t1.ZCFLAG)=0 and t1.ZCFLAG is null                                                   "+
                " AND t1.ZBPARTNE3 <> 'NULL' and  length(t1.ZBPARTNE3) <> 0 and t1.ZBPARTNE3 is not null "+
                " AND T1.ZKTGRM IN ('00001','00004')                                                                                       "+
                " AND T1.ZODTYPE IN ('Z001','Z002','Z011','Z012','Z013','Z014')) t4                                                        "+
                "GROUP BY t4.BPARTNER                                                                                                      "+
                ") t                                                                                                                       "
      HiveContextUtil.exec(sc,sql.format(monthago,timestr))
      
      val sql1 = "insert into table tmpparquet.t_30_zhyzcd                                                                                          "+
               "SELECT t.ZBPARTNE3 as BPARTNER,'10' as ZHY_ZCD,unix_timestamp() as create_time FROM parquetustag.PZBPARTNE3 t                       "+
               "where t.ZHYZCDT >= %s and t.ZHYZCDT <= %s"
      HiveContextUtil.exec(sc,sql1.format(monthago,timestr))
      
      //创建zhy_zcd指标_汇总表
      HiveContextUtil.exec(sc, "truncate table tmpparquet.t_30_zhyzcd_hz")
      
      val sql2 ="insert overwrite table tmpparquet.t_30_zhyzcd_hz                             "+
               "select b.bpartner,b.zhy_zcd                                 "+
               "from                                                        "+
               "(select c.bpartner,max(c.create_time) as create_time        "+
               "from tmpparquet.t_30_zhyzcd c                                          "+
               "group by c.bpartner) a, tmpparquet.t_30_zhyzcd b                        "+
               "where a.bpartner=b.bpartner and a.create_time=b.create_time "
      HiveContextUtil.exec(sc,sql2)
      
      val sql3 ="insert into table tmpparquet.t_30_zhyzcd_hz                                                                                       "+
               "select t.* from (                                                                                                      "+
               "SELECT t.ZBPARTNE3 as BPARTNER,'40' as ZHY_ZCD FROM tmpparquet.TAG_GWCZ t                                                         "+
               "where t.ZLAST_GW >= %s and t.ZLAST_GW <= %s "+
               "union all                                                                                                              "+
               "SELECT t.ZBPARTNE3 as BPARTNER,'50' as ZHY_ZCD FROM tmpparquet.TAG_GWCZ t                                                         "+
               "where  t.ZLAST_GW >= %s and t.ZLAST_GW <= %s "+
               "union all	                                                                                                             "+
               "SELECT t.ZBPARTNE3 as BPARTNER,'60' as ZHY_ZCD FROM tmpparquet.TAG_GWCZ t                                                         "+
               "where t.ZLAST_GW < %s"+
               ") t                                                                                                                    "

      HiveContextUtil.exec(sc,sql3.format(threemonthago,monthago,sixmonthago,threemonthago,sixmonthago))
      
      //最终展示
      HiveContextUtil.exec(sc, "truncate table tmpparquet.TAG_30")
            
      val sql4 ="insert overwrite table tmpparquet.TAG_30                                                            "+
               "select a.bpartner,a.zhy_zcd,b.zhy_nl,b.zhy_hl                                            "+
               "from tmpparquet.t_30_zhyzcd_hz a                                                         "+
               "left join                                                                                "+
               "(select t.BPARTNER,max(t.ZHY_NL) as ZHY_NL,max(t.ZHY_HL) as ZHY_HL                       "+
               "from (                                                                                   "+
               "SELECT t1.ZBPARTNE3 as BPARTNER,CASE WHEN t1.NL <18 THEN '10'                            "+
               "WHEN t1.NL >=18 AND t1.NL < 23 THEN '20'                                                 "+
               "WHEN t1.NL >=23 AND t1.NL < 30 THEN '30'                                                 "+
               "WHEN t1.NL >=30 AND t1.NL < 40 THEN '40'                                                 "+
               "WHEN t1.NL >=40 AND t1.NL < 50 THEN '50'                                                 "+
               "ELSE '60' END AS ZHY_NL,                                                                 "+
               "CASE WHEN t1.HL <=30 THEN '10'                                                           "+
               "WHEN t1.HL >30 AND t1.HL < 365 THEN '20'                                                 "+
               "WHEN t1.HL >=365 AND t1.HL < 730 THEN '30'                                               "+
               "WHEN t1.HL >=730 AND t1.HL < 1095 THEN '40'                                              "+
               "WHEN t1.HL >=1095 THEN '50'  END AS ZHY_HL                                               "+
               "FROM (                                                                                   "+
               "SELECT t.ZBPARTNE3,t.ZHYBIRTH AS SR,                                                     "+
               "       datediff(TO_DATE(from_unixtime(unix_timestamp())),                                "+
               "       to_date(from_unixtime(unix_timestamp(t.ZHYBIRTH, 'yyyyMMdd'))))/365 AS NL,        "+
               "       datediff(TO_DATE(from_unixtime(unix_timestamp())),                                "+
               "       to_date(from_unixtime(unix_timestamp(t.ZHYZCDT, 'yyyyMMdd')))) AS HL              "+
               " FROM parquetustag.PZBPARTNE3 t WHERE t.ZHYBIRTH <> '00000000') t1                       "+
               " ) t                                                                                     "+
               "group by t.BPARTNER) b                                                                   "+
               "on a.bpartner=b.bpartner                                                                "


      HiveContextUtil.exec(sc,sql4)
      
  }
                  
}