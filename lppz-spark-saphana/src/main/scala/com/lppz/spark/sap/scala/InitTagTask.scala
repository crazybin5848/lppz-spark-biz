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

object InitTagTask extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName) 
  
  
  def main(args : Array[String])  = {
  
      val sparkConf = new SparkConf().setAppName("InitTagTask")
                      .setMaster(args(1))
      val sc = new MysqlSpark().buildSc("InitTagTask", args(1))
      val bclass = new Hive2DbmsBean().getClass()
      val bean = SparkYamlUtils.loadYaml(args(0), false,bclass)
      val calen3 = Calendar.getInstance()
          calen3.add(Calendar.MONTH,-3)
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val timestr = sdf.format(new Date().getTime()).toString()
      val currmonth = timestr.substring(0, 6)
      val threemonthago = sdf.format(calen3.getTime()).toString()
      val monthago = threemonthago.substring(0, 6)
      
      
      HiveContextUtil.exec(sc, "use utags")
      val he = HiveContextUtil.execQuery(sc, "select * from zshopnew limit 10")
      he.foreach { x => println(x) }
      HiveContextUtil.exec(sc,"cache table mater_cache as select * from ZMATERIAL");
      HiveContextUtil.exec(sc,"cache table shop_cache as select * from ZSHOPNEW");
      
      HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
      
      HiveContextUtil.exec(sc, "truncate table tmpparquet.t_ZCMSD002")
      val sql2 = "insert overwrite table tmpparquet.t_ZCMSD002                                                                                        "+
         "select t1.*,t3.zmm_syb,t3.zxsqd,t2.zmm_szb ,t2.zwldl ,t2.ZKTGRM,t2.ZMM_SPKW                                          "+
         "from (select  calday,zorder,zbpartne3,zmaterial,zsalexd,ztimerag,zfgzr,zfjjr,zaramt,ZSHOPNEW,zck05,ZCFLAG,ZODTYPE "+
         "from  AZPWSD00800  where ds >= %s and ds <= %s                                                                                               "+
         "union                                                                                                             "+
         "select  calday,zorder,zbpartne3,zmaterial,zsalexd,ztimerag,zfgzr,zfjjr,zaramt,ZSHOPNEW,zck05,ZCFLAG,ZODTYPE       "+
         "from  AZPWSD00900  where ds >= %s and ds <= %s and calday >= %s and calday <= %s                                                                                               "+
         "union                                                                                                             "+
         "select  calday,zorder,zbpartne3,zmaterial,zsalexd,ztimerag,zfgzr,zfjjr,zaramt,ZSHOPNEW,zck05,ZCFLAG,ZODTYPE       "+
         "from  AZPWSD00600  where ds >= %s and ds <= %s                                                                                               "+
         ") t1                                                                                                              "+
         "left join mater_cache t2 on  (t1.zmaterial = t2.ZMATERIAL)                                                         "+
         "left join shop_cache t3 on  (t1.ZSHOPNEW = t3.ZSHOPNEW)                                                           "

        HiveContextUtil.exec(sc,sql2.format(threemonthago,timestr,monthago,currmonth,threemonthago,timestr,threemonthago,timestr))
        
        HiveContextUtil.exec(sc,"uncache table mater_cache")
        HiveContextUtil.exec(sc,"uncache table shop_cache")

  }
                  
}