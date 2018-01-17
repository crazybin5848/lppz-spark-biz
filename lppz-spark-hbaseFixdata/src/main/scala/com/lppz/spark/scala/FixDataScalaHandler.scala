package com.lppz.spark.scala

import java.lang._
import java.lang._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import com.lppz.spark.scala.jedis.JedisClientUtil
import com.lppz.spark.util.HbaseHandlerHttp

/**
 * @author chenlisong

 */
class FixDataScalaHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName)
  def exec(sc: SparkContext, sqlStr: String,hbasePath: String, filePath: String) {
    val d = HiveContextUtil.getRDD(sc, sqlStr)
    
    d.rdd./*repartition(16).*/foreachPartition { partitionOfRecords => 
      var handler: HbaseHandlerHttp = new HbaseHandlerHttp(filePath);
      var list: java.util.List[String] = new java.util.ArrayList()
      partitionOfRecords.foreach(record => {
        var orderId: String = record.getAs[String]("orderid");
        list.add(orderId)
        if(list.size() > 200) {
          handler.handle(list)
          list = new java.util.ArrayList()
        }
      }
      )
      
      //不足100的数据处理
      if(list.size() > 0) {
          handler.handle(list)
          list = new java.util.ArrayList()
        }
    }
    
  }
}