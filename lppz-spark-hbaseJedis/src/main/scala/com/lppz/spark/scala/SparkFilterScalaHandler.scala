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

/**
  * @author zoubin
  * 0 mode  local[16]
  * 1 partition 100
  * 2 filePath  /opt/fuck1w.txt
  * //3 jedisClusterPath  /Users/zoubin/Documents/scalawork/lppz-spark-biz/lppz-spark-hbaseJedis/src/main/resources/META-INF/jedis-cluster.yaml
  */
object SparkFilterScalaHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName)
  def main(args : Array[String]):Unit  = {
	  val mode=args(0)
		val partition: Int = Integer.parseInt(args(1))
    val filePath: String = args(2)
    //jedisClusterPath = args(3)
    
    val redisTimeKey = "hbaseJedis::time";
    val redisOrderidsKey = "hbaseJedis::filter::orderids";
    val colArrayKey = "hbaseJedis::filter::colArray";
    val sparkConf = new SparkConf().setAppName("InitTagTask")
      .setMaster(mode)
    val sc = new MysqlSpark().buildSc("InitTagTask", mode)
    val hiveRdd = sc.textFile("file:///home/hadoop/filterData/diffqj17_2.txt", 2)
    //1. 遍历hive rdd写入缓存，生成colArray
    hiveRdd.repartition(partition).foreachPartition { dd => {
    
      //instance redis
      object InternalRedisClient extends JedisClientUtil {
      }
      val jedisClusterPath = "/home/hadoop/filterData/jedis-cluster.yaml"
      InternalRedisClient.instance.makeJedisCluster(jedisClusterPath)
      val jedisCluster = InternalRedisClient.instance.getJedisCluster
      
      //col array
      val sb=new StringBuilder("orderid in (")
      if(dd!= null && !dd.isEmpty){
        dd.foreach { d => {
          sb.append("'").append(d.toString()).append("'").append(",")
        }}
        var index: Int = dd.indexOf() + 1;
        jedisCluster.lpush(colArrayKey, sb.toString().substring(0, sb.length-1) + ")");
      }
    }}

//    var list: java.util.List[String] = jedisCluster.lrange(colArrayKey, 0L, -1L);
//
//    var colArrays: Array[String] = new Array[String](list.size());
//    for(i<-0 to list.size()-1) {
//      colArrays.update(i, list.get(i));
//    }

//    println("colArraysSize:"+colArrays.size);
//    //2. 生成mysql，得到mysql的dateset
//    var mysqlDF = getMysqlListArrayDF("jdbc:mysql://10.16.3.109:3306/omsext", "order_sharding", colArrays, "oms_109", "oms_109", sc)
//    val rrdd=mysqlDF.rdd
////    println(rrdd.count())
//    rrdd.repartition(partition).foreachPartition { row =>
//      object InternalRedisClient extends JedisClientUtil {
//      }
//      InternalRedisClient.instance.makeJedisCluster(jedisClusterPath)
//      val jedisCluster = InternalRedisClient.instance.getJedisCluster
//      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
//
//      row.foreach(record => {
//        var createtime:java.sql.Timestamp = record.getAs[java.sql.Timestamp]("createtime");
//        var orderid:String = record.getAs[String]("orderid");
//        jedisCluster.incr(redisTimeKey + sdf.format(createtime));
//        jedisCluster.set(redisOrderidsKey + "::"+orderid, "ok");
//      }
//      )
//    }
//    println("xxxxxxxxx2");
//
//    //3. redis去重
//    hiveRdd.repartition(partition).foreachPartition { row =>
//      object InternalRedisClient extends JedisClientUtil {
//      }
//      InternalRedisClient.instance.makeJedisCluster(jedisClusterPath)
//      val jedisCluster = InternalRedisClient.instance.getJedisCluster
//
//      row.foreach { record =>
//        if(!"ok".equals(jedisCluster.get(redisOrderidsKey + "::"+record))) {
//          jedisCluster.lpush(redisOrderidsKey, record);
//        }
//      }
//    }
//    println("xxxxxxxxx3");
//
//    //4. 从redis获取time:count and orderids:list
//    var timeSet: java.util.Set[String] = jedisCluster.keys(redisTimeKey + "*");
//    for(key <- timeSet) {
//      println("time count, key:" +key+", count:"+jedisCluster.get(key));
//    }
//    
//    var flag: Boolean = true;
//    var count: Int = 0;
//    print("noMatchOrderids: ");
//    while(flag) {
//      var orderid: String = jedisCluster.rpop(redisOrderidsKey);
//      if(orderid!=null && !orderid.isEmpty()) {
//        if(count % 1000 == 0) {
//          println(orderid+", ");
//        }else {
//          print(orderid+", ");
//        }
//        count = count + 1;
//      }else {
//        flag = false;
//      }
//    }
//    println();
//    println("noMatchOrderids count: " + count);
    
    
//    var noMatchOrderids: java.util.List[String] = jedisCluster.lrange(redisOrderidsKey, 0, 1);
//    print("noMatchOrderids: ");
//    for(orderid <- noMatchOrderids) {
//      print(orderid+", ");
//    }
//    println();
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

}