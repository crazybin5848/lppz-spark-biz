package com.lppz.spark.scala

import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import com.lppz.spark.util.SparkHiveUtil
import com.lppz.core.datasource.DynamicDataSource
import java.sql.PreparedStatement
import org.apache.log4j.Logger
import java.sql.Connection
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.FSDataOutputStream

/**
 * @author zoubin
 */
class HistoryOrderDelHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName)
  
  def write2Hdfs(filePath:String,content:String){
    val conf = new Configuration();
    conf.setBoolean("dfs.support.append", true)
    val fs=FileSystem.get(URI.create(filePath), conf)
    val path=new Path(filePath)
    
    var out:FSDataOutputStream=null
    
    if(fs.exists(path)){
      out=fs.append(path)
    }else{
      out=fs.create(path)
    }
    
    IOUtils.write(content,out)
    IOUtils.closeQuietly(out)
  }
  
  def execQueryAndExecDelSql(sc: SparkContext, sqlStr: String,ds:DynamicDataSource) {
	  val d = HiveContextUtil.getRDD(sc, sqlStr)
    def execDelFun(iterator: Iterator[(String, String)]): Unit = {
	     var ps:PreparedStatement=null
	    	 var conn:Connection=null
	    	 var sql=""
	    	 var schemaName=""
    try {
      iterator.foreach(data => {
//         schemaName=data._1
//         sql=data._2
//         val dataSource:LppzMycatDataSource=ds.asInstanceOf[MycatClusterDynamicDataSource].fetchLoadDs()
//         conn=JdbcUtils.createConnectionFactory(dataSource.getJdbcUrl, dataSource.getProps)()
////         conn=dataSource.getConnection
//         ps=conn.prepareStatement(sql)
//         val num=ps.executeUpdate()
//         LOG.info("executing sql:"+schemaName+"::effect:"+num+",Success:"+sql)
      }
      )
    } catch {
      case e: Exception => {
        LOG.error(e.getMessage,e)
        LOG.error("execFailSql:"+sql)
        write2Hdfs("hdfs://hamaster:9000/tmp/execFailSql/"+schemaName+"/"+sql.split(" ")(2)+"/"+System.currentTimeMillis()+".sql",sql+";")
      }
    } 
    finally{
      if(ps!=null)
        ps.close()
      if(conn!=null)
        conn.close()
    }
  }
	d.rdd.collect().foreach { r => {
     val schema:String = r.get(r.fieldIndex("schema")).asInstanceOf[String]
     val tbl:String = r.get(r.fieldIndex("tbl")).asInstanceOf[String]
     if("order_sharding".equals(tbl.asInstanceOf[String].toLowerCase())){
       val delSql=buildDelSql(r)
       execDelFun(List((schema,delSql.toString())).iterator)
     }
     else{
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
     }
    }}
  }
  
  def buildNewDelRdd(sc: SparkContext, sqlStr: String, fileName: String) = {
	  val d = HiveContextUtil.getRDD(sc, sqlStr)
	  d.rdd.map {r =>buildDelBinSql(r)}.saveAsTextFile(fileName)
  }
  
  def buildDelSql(r:Row)={
    val sb: StringBuilder = new StringBuilder("delete from ")
    val schema = r.get(r.fieldIndex("schema"))
    val tbl = r.get(r.fieldIndex("tbl"))
    val col = r.get(r.fieldIndex("col"))
    sb.append(schema).append(".").append(tbl).append(" where ").append(col)
    if ("order_sharding".equals(tbl.asInstanceOf[String].toLowerCase())) {
      val cond = r.get(r.fieldIndex("cond"))
          sb.append(" ").append(cond).append(";")
    } else {
      val pk = r.get(r.fieldIndex("pk"))
          val colin = r.get(r.fieldIndex("colin"))
          sb.append(" in(").append(colin).append(");")
    }
    sb
  }
  
  def buildDelBinSql(r:Row)={
  val schema = r.get(r.fieldIndex("schema"))
  val tbl = r.get(r.fieldIndex("tbl"))
  val col = r.get(r.fieldIndex("col"))
  val sb: StringBuilder=new StringBuilder("")
  if ("order_sharding".equals(tbl.asInstanceOf[String].toLowerCase())) {
     sb.append("delete from ")
     sb.append(schema).append(".").append(tbl).append(" where ").append(col)
	    val cond = r.get(r.fieldIndex("cond"))
			sb.append(" ").append(cond).append(";")
  } else {
			 val colin:String = r.get(r.fieldIndex("colin")).asInstanceOf[String]
       val colinArray:Array[String]=colin.split(",")
       val col:String = r.get(r.fieldIndex("col")).asInstanceOf[String]
       var k=0
       val size=5000
       while(k<=colinArray.length){
         val end=if(k+size>colinArray.length) colinArray.length else k+size
         if(end-k>0){
           val subArray:Array[String]=new Array[String](end-k)
           Array.copy(colinArray,k,subArray,0,end-k)
           sb.append("delete from ").append(schema).append(".").append(tbl).append(" where `").append(col).append("` in(")
           for(j <- subArray.indices){
              sb.append("'").append(subArray.apply(j).replaceAll("'", "")).append("'")
              if(j<subArray.length-1)
              sb.append(",")
           }
           sb.append(");\n")
         }
         k+=size              
       }
  }
  sb
  }
}