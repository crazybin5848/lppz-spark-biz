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

object SqlExportTask extends Serializable {
  @transient lazy val LOG = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("SqlExportTask")
      .setMaster(args(1))
    val sc = new MysqlSpark().buildSc("SqlExportTask", args(1))
    val bclass = new Hive2DbmsBean().getClass()
    val bean = SparkYamlUtils.loadYaml(args(0), false, bclass)
    val calen3 = Calendar.getInstance()
    calen3.add(Calendar.MONTH, -3)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val timestr = sdf.format(new Date().getTime()).toString()
    val currmonth = timestr.substring(0, 6)
    val threemonthago = sdf.format(calen3.getTime()).toString()
    val monthago = threemonthago.substring(0, 6)

    HiveContextUtil.exec(sc, "use omsext")

    val sql2 = """SELECT
      a.outorderid,
      a.username,
      null as aa,
      a.paidamount,
      a.carriagefee ,
      null as bb,
      a.paidamount,
      null as cc,
      a.orderpayment,
      null as dd,
      '交易成功' AS ee,
      a.buyermemo ,
      a.shippingfirstname,
      a.shad_addressLine2 ,
      '快递' as ff,
      a.shad_phoneNumber,
      a.shad_mobilephone,
      a.creationtime AS gg,
      a.paymentdate AS hh,
      null AS ii,
      (COUNT(DISTINCT (CASE WHEN b.bundleproductid IS NOT NULL AND bundleproductqty IS NOT NULL THEN b.bundleproductid END)) + 
      COUNT(DISTINCT (CASE WHEN b.bundleproductid IS NULL AND bundleproductqty IS NOT NULL THEN b.skuid END))
      ) AS jj,
      c.trackingid ,
case c.logisticscode 
       when '0009000015' then '中通' 
       when '0009000016' then '顺丰'
       when '0009000020' then '京东配送'
       when '0009000021' then '顺丰冷链'
       when '0009000022' then '韵达'
       when '0009000024' then '邮政快递包裹'
       when '0009100058' then '百世快递'
       when '0009000001' then '圆通'
       when '0009000002' then 'EMS'
       when '0009000004' then '申通' else 'other' end as wl,
      a.sellermemo ,
      (SUM(CASE WHEN bundleproductqty = 0 AND b.bundleproductqty IS NOT NULL THEN b.quantityvalue END) + 
      SUM(CASE WHEN bundleproductqty != 0 AND b.bundleproductqty IS NOT NULL THEN b.bundleproductqty END)) AS kk,
      null AS ll,
      null AS mm
FROM omsext.omsextorders a 
LEFT JOIN omsext.omsextorderlines b ON b.myorder = a.id
LEFT JOIN omsext.omsextbusilpdeliveryedata c ON c.myorder = a.id 
where a.state IN ('OUT_STORAGE','RECEIVED')
AND a.paymentdate <= '2016-09-18 08:58:42'
and a.paymentdate >= '2015-01-01 00:00:00'
AND a.basestore ='single|BaseStoreData|1007'
GROUP BY a.outorderid,
         a.username,
         a.paidamount,
         a.carriagefee,
         a.paidamount,
         a.orderpayment,
         a.buyermemo ,
         a.shippingfirstname,
         a.shad_addressLine2,
         a.shad_phoneNumber,
         a.shad_mobilephone,
         a.creationtime,
         a.paymentdate,
         c.trackingid ,
         case c.logisticscode 
                when '0009000015' then '中通' 
                when '0009000016' then '顺丰'
                when '0009000020' then '京东配送'
                when '0009000021' then '顺丰冷链'
                when '0009000022' then '韵达'
                when '0009000024' then '邮政快递包裹'
                when '0009100058' then '百世快递'
                when '0009000001' then '圆通'
                when '0009000002' then 'EMS'
                when '0009000004' then '申通' else 'other' end ,
                 a.sellermemo"""
    val dataframe = HiveContextUtil.getRDD(sc, sql2)

    val fileName = "/tmp/exportorder/order.txt"
    val destFileName = bean.getHdfsUrl + "/tmp/Genexportorder/ord.txt"
    val util = new BuildLoadFileUtil()
    dataframe.rdd.repartition(100).map { row =>
      val sql = util.buildLoadFile(row)
      sql
    }.saveAsTextFile(fileName)

    val mysql = new MysqlSpark()

    var hdfs: FileSystem = null

    hdfs = new SparkHdfsUtil().getFileSystem(bean.getHdfsUrl)

    mysql.mergeHdfsFile(hdfs, destFileName, fileName, bean.getHdfsUrl)

  }

}