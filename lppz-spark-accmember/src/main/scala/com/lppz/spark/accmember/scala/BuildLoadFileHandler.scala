package com.lppz.spark.accmember.scala

import com.lppz.spark.accmember.BuildLoadFileUtil
import com.lppz.spark.accmember.bean.TableBean
import java.util.HashMap
import org.apache.spark.sql.Row
import com.lppz.spark.util.jedis.SparkJedisCluster

class BuildLoadFileHandler extends Serializable{
  object instance extends Serializable {
    def handler(tableName:String,table:TableBean,enumTable:HashMap[String,String], r: Row,jedisCluster:SparkJedisCluster)={
      val handler=new BuildLoadFileUtil()
      
      val sql=handler.buildLoadFile(tableName, table, enumTable, r,jedisCluster)
      
      sql
    }
  }
  
}