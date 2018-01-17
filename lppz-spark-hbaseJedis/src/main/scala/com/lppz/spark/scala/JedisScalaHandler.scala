package com.lppz.spark.scala

import java.util.ArrayList
import java.util.HashMap
import java.util.UUID
import java.net.URI
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
import scala.collection.mutable.ArrayBuffer
import java.sql.Connection
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.FSDataOutputStream
import com.lppz.spark.scala.jedis.JedisClientUtil

/**
 * @author zoubin
 */
class JedisScalaHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName)
  def exec(sc: SparkContext, sqlStr: String,jedisClusterYamlPath: String) {
	  val d = HiveContextUtil.getRDD(sc, sqlStr)
    def execDelFun(iterator: Iterator[(String, String)]): Unit = {
      iterator.foreach(data => {
         val schemaName=data._1
         val sql=data._2
         object InternalRedisClient extends JedisClientUtil {
          }
         InternalRedisClient.instance.makeJedisCluster(jedisClusterYamlPath)
         val jedisCluster=InternalRedisClient.instance.getJedisCluster
         jedisCluster.lpush("bfuck",schemaName+":::"+sql)
      }
      )
  }
	  
	d.rdd.collect().foreach { r => {
     val schema:String = r.get(r.fieldIndex("schema")).asInstanceOf[String]
     val tbl:String = r.get(r.fieldIndex("tbl")).asInstanceOf[String]
       val colin:String = r.get(r.fieldIndex("colin")).asInstanceOf[String]
       val colinArray:Array[String]=colin.split(",")
       val col:String = r.get(r.fieldIndex("col")).asInstanceOf[String]
       var k=0
       val size=100000
       while(k<=colinArray.length){
         val end=if(k+size>colinArray.length) colinArray.length else k+size
         if(end-k>0){
         val subArray:Array[String]=new Array[String](end-k)
         Array.copy(colinArray,k,subArray,0,end-k)
         val colString=SparkHiveUtil.buildStringArray(subArray, 100, col,tbl)
         val ll=colString.map{subcol => (schema,subcol)}
         sc.parallelize(ll).foreachPartition(execDelFun)
         }
         k+=size
       }
    }}
  }
}