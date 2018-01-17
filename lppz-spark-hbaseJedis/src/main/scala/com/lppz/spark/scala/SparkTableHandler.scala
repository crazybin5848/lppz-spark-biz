package com.lppz.spark.scala

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import com.lppz.spark.scala.jedis.JedisClientUtil
import com.lppz.spark.util.SparkHiveUtil
import java.util.HashMap
import java.util.UUID
import org.apache.spark.SparkConf
import com.lppz.spark.scala.jdbc.MysqlSpark
import com.lppz.spark.bean.SparkSqlConfigBean
import com.lppz.spark.bean.SparkMysqlDmlBean
import java.util.Properties
import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import java.util.Arrays.ArrayList

/**
  * @author zoubin
  */
object SparkTableHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName)

  var jedisClusterPath = "/home/hadoop/filterData/jedis-cluster.yaml"
  
  /**
   * 0  mode  local
   * 1  partition  10
   * 2  tableName orders
   * 3  hdfsFilePath  /tmp/
   * 4  hdfsUrl hdfs://hamaster:9000
   * 5  jdbcUrl  jdbc:mysql://10.16.3.109:3306/omsext
   * 6  jdbcUser  oms_109
   * 7  jdbcPwd oms_109
   * //8  jedisClusterPath   /Users/chenlisong/Documents/lppz_code/lppz-spark-biz/lppz-spark-hbaseJedis/src/main/resources/META-INF/jedis-cluster.yaml
   * 8  colBegin  0
   * 9  colEnd  -1
   */
  def main(args : Array[String]):Unit  = {
    val mode: String = args(0)
    val tableName: String = args(1)
    val hdfsFilePath = args(2)
    val fileName: String = hdfsFilePath+tableName
    val hdfsUrl: String = args(3)
    val jdbcUrl: String = args(4)
    val jdbcUser: String = args(5)
    val jdbcPwd: String = args(6)
//    val colBegin = Integer.parseInt(args(7))
//    val colEnd = Integer.parseInt(args(8))
    
    val colArrayKey: String = "hbaseJedis::filter::colArray";

    val sparkConf = new SparkConf().setAppName("InitTagTask")
      .setMaster(mode)
    val sc = new MysqlSpark().buildSc("InitTagTask", mode)
    
    //val hiveRdd = sc.textFile(filePath, 2)

    val map:HashMap[String,String]=new HashMap[String,String]()

    val jedisCluster = getJedisCluster()
    val colBegin = Integer.parseInt(jedisCluster.get("colBegin"))
    val colEnd = Integer.parseInt(jedisCluster.get("colEnd"))
    //清理redis数据
//    jedisCluster.del(colArrayKey);

    //1. 遍历hive rdd写入缓存，生成colArray
//    hiveRdd.repartition(partition).foreachPartition { dd => {
//      val jedisCluster=getJedisCluster()
//
//      //col array
//      val sb=new StringBuilder("orderid in (")
//      if(dd!= null && !dd.isEmpty){
//        dd.foreach { d => {
//          sb.append("'").append(d.toString()).append("'").append(",")
//        }}
//        //map.put(UUID.randomUUID().toString(), sb.toString())
//        var index: Int = dd.indexOf() + 1;
//        jedisCluster.lpush(colArrayKey, sb.toString().substring(0, sb.length-1) + ")");
//      }
//    }}

    var list: java.util.List[String] = jedisCluster.lrange(colArrayKey, colBegin, colEnd);

    var colArrays: Array[String] = new Array[String](list.size());
    for(i<-0 to list.size()-1) {
      colArrays.update(i, list.get(i));
    }

    //println("colArrays:"+colArrays(0))

    //2. 生成mysql，得到mysql的dateset
    var mysqlDF = getMysqlListArrayDF("jdbc:mysql://10.16.3.109:3306/omsext", tableName, colArrays, "oms_109", "oms_109", sc)
    mysqlDF.rdd.map { r =>
        val s: StringBuilder = new StringBuilder("")
        for (j <- Range(0, r.length)) {
          var o = r.get(j)
          if (o == null)
            o = "\\N";
          if (o.isInstanceOf[String]) {
            if ("null".equals(o.asInstanceOf[String].toLowerCase()))
              o = "\\N"
            o = (o.asInstanceOf[String]).replaceAll("\r", "")
            o = (o.asInstanceOf[String]).replaceAll("\n", "")
            o = (o.asInstanceOf[String]).replaceAll("\t", " ")
          }
          s.append(o)
          if (j < r.length - 1)
            s.append("\t")
        }
        s
    }.saveAsTextFile(fileName)

    var hdfs:FileSystem=null
    hdfs=new SparkHdfsUtil().getFileSystem(hdfsUrl)
    val destFileName = "/tmp/gen/" + tableName + "_" + colBegin + ".txt"

    val mysql=new MysqlSpark()
    mysql.mergeHdfsFile(hdfs, destFileName, fileName, hdfsUrl)
  }
  
  /**
   * 返回colArrays结果
   * colName 字段名：orderid
   * splitNumber: 超过多少数量分割
   * 
   * select * from (select row_number() over (order by orderid) as rnum ,table.* from diffqj17_2)t where rnum betwneen 1 to 10
   */
  def queryColArrays(sc: SparkContext, tableName: String, colName: String, splitNumber: Int, indexBegin: Int, indexEnd: Int): java.util.List[String] ={
    var sql: String = "select * from (select row_number() over (order by "+colName+") as rnum ,table.* from "+tableName+")t where rnum betwneen "+indexBegin+" to "+indexEnd;
    val rdd = HiveContextUtil.getRDD(sc, sql);

    var sb=new StringBuilder("orderid in (")
    var colArrays: java.util.List[String] = new java.util.ArrayList();
    var count: Int = 0;
    rdd.foreach { row => 
      
      var orderid:String = row.getAs[String](colName);
      sb.append("'").append(orderid).append("'").append(",")
      if(count % splitNumber == 0) {
        colArrays.add(sb.toString().substring(0, sb.length-1) + ")")    
        sb = new StringBuilder("orderid in (")
      }
      count = count + 1;
    }
    
    if(sb.length > "orderid in (".length()) {
      colArrays.add(sb.toString().substring(0, sb.length-1) + ")")    
    }
    colArrays
  }

  def getMysqlListArrayDF(url: String, tableName: String, colArray: Array[String], user: String, pwd: String, sc: SparkContext) = {
    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", pwd)
    val sqlContext = new SQLContext(sc)
    val jdbcDF = sqlContext.read.jdbc(url, tableName, colArray, prop)
    jdbcDF.createOrReplaceTempView(tableName)
    jdbcDF
  }

  def getJedisCluster() = {
    //instance redis
    object InternalRedisClient extends JedisClientUtil {
    }
    InternalRedisClient.instance.makeJedisCluster(jedisClusterPath)
    InternalRedisClient.instance.getJedisCluster
  }

}