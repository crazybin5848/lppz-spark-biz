package com.lppz.spark.mysql.scala

import org.apache.log4j.Logger
import com.lppz.spark.mysql.bean.Db2DbTableBean
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.lppz.spark.mysql.bean.Db2DbBean
import java.util.Properties
import com.lppz.spark.scala.jdbc.SparkJdbcTemplete
import java.util.ArrayList
import com.lppz.spark.bean.jdbc.JdbcDBTemplate
import com.lppz.spark.scala.jdbc.JdbcSqlHandler
import org.apache.spark.sql.Row
import com.lppz.spark.scala.kafka.KafkaProducerUtil
import com.lppz.spark.kafka.SparkExportStringProducer
import com.lppz.spark.scala.rocketmq.RocketMqProducerUtil

class Db2DbWithKafkaHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName)
  
  def startExportWithView(sc: SparkContext, bean:Db2DbBean,table:Db2DbTableBean,dataSourcePath:String,kafkaBrokerPath:String,lowerBound:Long,upperBound:Long){
    val sourceDriver = bean.getSourceDriver
    val sourceUrl = bean.getSourceJdbcUrl
    val sourceUser = bean.getSourceUser
    val sourcePwd = bean.getSourcePwd
    
    val sqlContext = new SQLContext(sc)
    
    val prop = new Properties()
    prop.put("user", bean.getSourceUser)
    prop.put("password", bean.getSourcePwd)
    
    var df=sqlContext.read.jdbc(sourceUrl, table.getSourceTableOrView, table.getPrimaryKey, lowerBound, upperBound, bean.getPartition, prop)
    
//    if(df.rdd.isEmpty())
//      null
    
    df.createOrReplaceTempView(table.getSourceTableOrView)
    
    df=df.sqlContext.sql("select * from ".concat(table.getSourceTableOrView.concat(" where ")
        .concat(table.getPrimaryKey).concat(" between ").concat(String.valueOf(lowerBound)).concat(" and ").concat(String.valueOf(upperBound))))
    
    df.rdd.cache()
    
    var batchSize = table.getBatchSize
    
    df.rdd.repartition(table.getPartition).foreachPartition { it =>
        val sqlList = new ArrayList[String]()
//        object InternalSparkJdbcUtil extends SparkJdbcTemplete {}
//        
//        InternalSparkJdbcUtil.instance.buildJdbcTemplete(dataSourcePath)
//        val st=InternalSparkJdbcUtil.instance.getSt()
        
        object InternalSparkKafkaUtil extends KafkaProducerUtil{}
    
        InternalSparkKafkaUtil.instance.buildStringKafkaProducer(kafkaBrokerPath)
        
        val producer=InternalSparkKafkaUtil.instance.getStringProducer
        it.foreach { row =>
          var sql = buildInsertSql(table.getTargetTableName, table.getTargetColumns, table.getPrimaryKey,row)
          
          producer.sendMsg(sql)
        }
      }
  }
  
  def startExportWithRmq(sc: SparkContext, bean:Db2DbBean,table:Db2DbTableBean,nameNodeAddr:String,lowerBound:Long,upperBound:Long){
    val sourceDriver = bean.getSourceDriver
    val sourceUrl = bean.getSourceJdbcUrl
    val sourceUser = bean.getSourceUser
    val sourcePwd = bean.getSourcePwd
    
    val sqlContext = new SQLContext(sc)
    
    val prop = new Properties()
    prop.put("user", bean.getSourceUser)
    prop.put("password", bean.getSourcePwd)
    
    var df=sqlContext.read.jdbc(sourceUrl, table.getSourceTableOrView, table.getPrimaryKey, lowerBound, upperBound, bean.getPartition, prop)
    
//    if(df.rdd.isEmpty())
//      null
    
    df.createOrReplaceTempView(table.getSourceTableOrView)
    
    df=df.sqlContext.sql("select * from ".concat(table.getSourceTableOrView.concat(" where ")
        .concat(table.getPrimaryKey).concat(" between ").concat(String.valueOf(lowerBound)).concat(" and ").concat(String.valueOf(upperBound))))
    
    df.rdd.cache()
    
    var batchSize = table.getBatchSize
    
    df.rdd.repartition(table.getPartition).foreachPartition { it =>
        val sqlList = new ArrayList[String]()
//        object InternalSparkJdbcUtil extends SparkJdbcTemplete {}
//        
//        InternalSparkJdbcUtil.instance.buildJdbcTemplete(dataSourcePath)
//        val st=InternalSparkJdbcUtil.instance.getSt()
        
       object InternalProducerClient extends RocketMqProducerUtil {}
       InternalProducerClient.instance.buildRocketMqProducer(nameNodeAddr)
       val producer = InternalProducerClient.instance.getProducer
        it.foreach { row =>
          var sql = buildInsertSql(table.getTargetTableName, table.getTargetColumns, table.getPrimaryKey,row)
          
          producer.sendMsgConcurrenly(sql, table.getTargetTableName.concat("::").concat(row.getAs(table.getPrimaryKey)))
        }
      }
  }
  
  def buildInsertSql(tableName:String,columns:String,excludeColumn:String,r:Row):String={
    val sql:StringBuilder=new StringBuilder("INSERT INTO ")
    sql.append(tableName).append("(").append(columns).append(") values(")
    
    val idIndex=if(null!=excludeColumn && !"".equals(excludeColumn)) r.fieldIndex(excludeColumn) else -1
    
    for(j<- Range(0,r.length)){
      //剔除id字段
      if(j!=idIndex){
        var o = r.get(j)
        if (o == null || "".equals(o.toString())){
        sql.append("null")
        }else{
          var value = o.toString().replaceAll("'|/|\\\\", "");
          value = value.replaceAll("^\\s* |\\s*$", "");
          sql.append("'")
          sql.append(value)
          sql.append("'")
       }
       if (j < r.length - 1)
         sql.append(",")
      }
    }
    sql.append(")")
    sql.toString();
  }
  
}