package com.lppz.spark.mysql.scala;

import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.Properties

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.CustomizedJdbcPartition
import org.apache.spark.rdd.CustomizedJdbcRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import com.lppz.spark.mysql.bean.Db2DbBean
import com.lppz.spark.mysql.bean.Hive2DbmsBean
import com.lppz.spark.scala.SparkHdfsUtil
import com.lppz.spark.scala.jdbc.MysqlSpark
import com.lppz.spark.scala.jdbc.SparkJdbcTemplete
import java.util.ArrayList
import com.lppz.spark.scala.jdbc.JdbcSqlHandler
import org.apache.mesos.Log
import com.lppz.spark.bean.jdbc.JdbcDBTemplate
import com.lppz.spark.MultiMysql2MysqlSpark
import com.lppz.spark.mysql.bean.Db2DbTableBean
import com.lppz.core.datasource.LppzBasicDataSource
import org.springframework.jdbc.UncategorizedSQLException
import com.alibaba.fastjson.JSON
import scala.util.control.Breaks

class Db2DbHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName)
  
  def startExportWithView(sc: SparkContext, bean:Db2DbBean,table:Db2DbTableBean,dataSourcePath:String,lowerBound:Long,upperBound:Long,batch:Boolean){
    val sourceDriver = bean.getSourceDriver
    val sourceUrl = bean.getSourceJdbcUrl
    val sourceUser = bean.getSourceUser
    val sourcePwd = bean.getSourcePwd
    val tables = bean.getTables
    
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
        object InternalSparkJdbcUtil extends SparkJdbcTemplete {}
        
        InternalSparkJdbcUtil.instance.buildJdbcTemplete(dataSourcePath)
        val st=InternalSparkJdbcUtil.instance.getSt()
        it.foreach { row =>
          var sql = buildInsertSql(table.getTargetTableName, table.getTargetColumns, table.getPrimaryKey,row)
          LOG.debug("++++++++"+sql)
          sqlList.add(sql)
          if(sqlList.size()>=batchSize && !batch){
            batchInsert(sqlList, st);
          }
        }
        if(!sqlList.isEmpty() && !batch){
          LOG.info("not empty size " + sqlList.size())
        	batchInsert(sqlList, st);
        }
        if(batch && !sqlList.isEmpty()){
          InternalSparkJdbcUtil.instance.mulitExec(sqlList, batchSize)
        }
        st.close()
      }
  }
  
  def startExport(sc: SparkContext, bean:Db2DbBean,dataSourcePath:String) {
    val sourceDriver = bean.getSourceDriver
    val sourceUrl = bean.getSourceJdbcUrl
    val sourceUser = bean.getSourceUser
    val sourcePwd = bean.getSourcePwd
    val tables = bean.getTables
    
    for (i <- Range(0, tables.size())) {
      val table = tables.get(i);
      var batchSize = table.getBatchSize
      var parameters = Map[String, Object]();
      if(table.getStartTag != null){
         parameters += ("startTag" -> table.getStartTag);
      }
      if(table.getEndTag != null){
    	  parameters += ("endTag" -> table.getEndTag);
      }
      
      if(batchSize == null || batchSize==0){
        batchSize = 100
      }
      LOG.info("-------------------------------------------")
      LOG.info(table.getSourceSql)
      LOG.info(table.getPartition)
      LOG.info("-------------------------------------------")
      val rdd = exec(sc, sourceDriver, sourceUrl, sourceUser, sourcePwd, table.getSourceSql, parameters)
      rdd.repartition(table.getPartition).foreachPartition { it =>
        val sqlList = new ArrayList[String]()
        object InternalSparkJdbcUtil extends SparkJdbcTemplete {}
        
        InternalSparkJdbcUtil.instance.buildJdbcTemplete(dataSourcePath)
        val st:JdbcDBTemplate=InternalSparkJdbcUtil.instance.getSt()
        it.foreach { row =>
          var sql = buildInsertSql(table.getTargetTableName, table.getTargetColumns, row)
          LOG.debug("++++++++"+sql)
          sqlList.add(sql)
          if(sqlList.size()>=batchSize){
            batchInsert(sqlList, st);
          }
        }
        if(!sqlList.isEmpty()){
          LOG.info("not empty size " + sqlList.size())
        	 batchInsert(sqlList, st);
        }
        st.close()
      }
    }
    sc.stop()
  }
  
  def batchInsert(sqlList:ArrayList[String],st:JdbcDBTemplate){
    try {
		  val handler=new JdbcSqlHandler(sqlList.toArray(new Array[String](sqlList.size())))
		  st.doIntrans(handler)
		  LOG.info("++++++++++++sqlList size : " +sqlList.size())
		  sqlList.clear();
	  }catch {
	    case t: Throwable => {
	      LOG.error("batch insert exception ",t);
//	      throw new RuntimeException("batch insert exception",t);
	      var needTry=true
	      if(t.isInstanceOf[UncategorizedSQLException]){
	        val ee=t.asInstanceOf[UncategorizedSQLException]
					if(ee.getSQLException().getErrorCode()==1062){
						LOG.error(t.getMessage,t);
						LOG.error("no need to retry");
						needTry=false;
					}
	      }
	      if(needTry){
	        val loop = new Breaks;
          loop.breakable {
            for(i <- Range(1,21)){
						  LOG.info("jdbc batchUpdate retry no "+i);
						  val handler=new JdbcSqlHandler(sqlList.toArray(new Array[String](sqlList.size())))
						  st.doIntrans(handler)
        		  LOG.info("++++++++++++sqlList size : " +sqlList.size())
        		  sqlList.clear()
						  loop.break
					  }
          }
					
					LOG.error("Here is the failure of 20 times sql :");
				}
	    }
	  }
  }
  
  def buildInsertSql(tableName:String,columns:String,values:Array[Object]):String={
    val sql:StringBuilder=new StringBuilder("INSERT INTO ")
    sql.append(tableName).append("(").append(columns).append(") values(")
    var index = 0;
    values.foreach { x =>
      index+=1
      if("".equals(x)){
        sql.append("null")
      }else
      if(x != null){
        var value = x.toString().replaceAll("'|/|\\\\", "");
        value = value.replaceAll("^\\s* |\\s*$", "");
        sql.append("'")
    	  sql.append(value)
    	  sql.append("'")
      }else{        
    	  sql.append(x)
      }
      if(index<(values.size)){
        sql.append(",")
      }
    }
    sql.append(")")
    return sql.toString();
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
  
    def exec(sc: SparkContext,driverName:String,url:String,username:String,pwd:String,sql:String,parameters:Map[String, Object]):CustomizedJdbcRDD[Array[Object]]={
    Class.forName(driverName);
    LOG.info("driverName:"+driverName)
    LOG.info("url:"+url)
    LOG.info("username:"+username)
    LOG.info("pwd:"+pwd)
    LOG.info("sql:"+sql)
    LOG.info("parameters:"+parameters)
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
//    data.collect().foreach { x => x.foreach { y =>  println(y)} }
    return data
  }
  
}