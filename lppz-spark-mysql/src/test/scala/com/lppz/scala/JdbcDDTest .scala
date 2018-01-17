package com.lppz.scala

import java.sql.DriverManager
import org.apache.spark.SparkConf
import org.apache.spark.rdd.CustomizedJdbcRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.CustomizedJdbcPartition
import org.apache.spark.Partition
import java.sql.PreparedStatement

object JdbcDDTest {

	val url = "jdbc:mysql://192.168.19.74:3306/acc_70?useConfigs=maxPerformance&characterEncoding=utf8";
	val username = "acc";
	val driverName = "com.mysql.jdbc.Driver";
	val pwd = "acc123";
  def main(args: Array[String]): Unit = {
    queryByRange()
//     queryAll()
  }
  
  def queryByRange():CustomizedJdbcRDD[Array[Object]]={
    var sql = "SELECT a.p_lppartnerno as partner_no, a.p_lpcardno as card_no, b.p_memberno as member_no, a.p_lpmobileno as telephone, e.`Code` AS source_type, a.p_uid as member_account, a.passwd as member_pwd, a.p_passwordencoding as password_encoding, a.p_lpbirthday as birthday, a.createdTS as create_time, a.p_lpcreator as create_user, a.modifiedTS as edit_time, 'admin' as edit_user, a.p_lpregisterstore as register_store, a.p_lprecomder as recommender, a.p_lpmemberstatus as member_status, a.p_synccrmstatus as sync_crm_status, a.p_synccrmtime as sync_crm_time, c.`Code` AS member_type, d.`Code` AS member_level FROM lpmembers b INNER JOIN users a ON b.p_customer =a.PK LEFT JOIN enumerationvalues c ON c.PK = b.p_membercd LEFT JOIN enumerationvalues d ON d.PK = b.p_leveltype LEFT JOIN enumerationvalues e ON e.PK = b.p_sourcetype where a.createdts >=? and a.createdts<?";
    var parameters = Map[String, Object]();
      parameters += ("startTime" -> "2015-08-01");
      parameters += ("endTime" -> "2015-09-01");
      return  exec(sql, parameters);
  }
  
  def queryAll():CustomizedJdbcRDD[Array[Object]]={
    var sql="SELECT a.PK as id, c.p_lppartnerno as partner_no, b.p_memberno as member_no, c.p_lpcardno as card_no, e.`Code` AS level_type, '0' AS all_scores, '0' AS credit_scores, '0' AS behavior_scores, '0' AS all_card_balance, '0' AS card_balance, '0' AS donation_amount, IF(c.p_lpcardstatus<> 'X','Y','X') FROM lpmemberscores a INNER JOIN lpmembers b ON b.PK = a.p_lppzmembers INNER JOIN users c ON c.PK =b.p_customer LEFT JOIN enumerationvalues e ON e.PK=b.p_leveltype";
    var parameters = Map[String, Object]();
    return exec(sql, parameters);
  }
  
  def exec(sql:String,parameters:Map[String, Object]):CustomizedJdbcRDD[Array[Object]]={
        val conf = new SparkConf().setAppName("JdbcRDDTest").setMaster("local[2]");
    val sc = new SparkContext(conf);
    Class.forName(driverName);

    val data = new CustomizedJdbcRDD(sc,
      //创建获取JDBC连接函数
      () => {
        DriverManager.getConnection(url, username, pwd);
      },
      //设置查询SQL
      sql,
      //创建分区函数
      () => {
        val partitions = new Array[Partition](1);
        val partition = new CustomizedJdbcPartition(0, parameters);
        partitions(0) = partition;
        partitions;
      },
      //为每个分区设置查询条件(基于上面设置的SQL语句)
      (stmt: PreparedStatement, partition: CustomizedJdbcPartition) => {
    	  val params = partition.asInstanceOf[CustomizedJdbcPartition].partitionParameters
        if(params.size > 0){
          var index=0;
          params.keys.foreach { key => 
            index+=1
            stmt.setString(index,params.get(key).get.asInstanceOf[String])
          }
        }
        stmt;
      });
//    data.collect().foreach { x =>
//    		    x.foreach { y =>  
//    		      print(y)
//    		      print("||")  
//    		    } 
//    		    println()
//    		    }
//    data.collect().foreach { x =>
//    println(x)
//    }
    data.repartition(2).foreachPartition { x => 
      println("--------------------------")
       x.foreach { y =>  
    		      y.foreach { z =>  
    		      print(z)
    		      print("||")  
    		    } 
    		      println()
    		    } 
    }
    return data
  }

}