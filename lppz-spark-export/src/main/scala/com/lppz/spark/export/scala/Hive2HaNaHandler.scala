package com.lppz.spark.export.scala;

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
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
import com.lppz.spark.export.handler.HaNaJdbcHandler
import org.apache.hadoop.fs.FSDataInputStream
import java.io.OutputStream
import java.io.FileOutputStream
import org.apache.hadoop.io.IOUtils
import java.io.InputStream
import java.io.File
import org.apache.hadoop.fs.LocalFileSystem
import java.io.BufferedOutputStream

class Hive2RdbmsHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName)
  
  def buildMysqlLoadFile(appName: String,bean:Hive2DbmsBean,sc:SparkContext)={
    val url=bean.getHdfsUrl
    
    val conf = new Configuration()
    
    val hdfs=FileSystem.get(URI.create(url),conf)
    
    val list=new ArrayList[String]()
    
    if(hdfs.isDirectory(new Path(bean.getHiveUrl))){
      
      val iterator=hdfs.listFiles(new Path(bean.getHiveUrl), false)
      
      while(iterator.hasNext()){
        val fileName=iterator.next().getPath.getName
        
        val rdd=sc.textFile(bean.getHdfsUrl+bean.getHiveUrl+"/"+fileName, 100)

        rdd.collect().foreach { s => {
          val sb:StringBuilder=new StringBuilder()
          
          sb.append("(")
          
          val arr=s.split("\t")
          
          for(i<-0 until arr.length){
            if(i==0){
              sb.append("'").append(arr(i)).append("'")
            }else{
              sb.append(",'").append(arr(i)).append("'")
            }
          }
          sb.append(")")
          
          list.add(sb.toString)
        } }
      }
      
    }else{
      val rdd=sc.textFile(bean.getHdfsUrl+bean.getHiveUrl, 100)
      
      rdd.collect().foreach { s => {
          val sb:StringBuilder=new StringBuilder()
          
          sb.append("(")
          
          val arr=s.split("\t")
          
          for(i<-0 until arr.length){
            if(i==0){
              sb.append("'").append(arr(i)).append("'")
            }else{
              sb.append(",'").append(arr(i)).append("'")
            }
          }
          sb.append(")")
          
          list.add(sb.toString)
        } }
      
    }
    
    list
  }
  
  def loadWithJdbc(appName: String,bean:Hive2DbmsBean,sc:SparkContext)={
    HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
    
    val arrayRows=HiveContextUtil.execQuery(sc, bean.getHiveSql)
      
//    val lists=Arrays.asList(arrayRows.toArray)
//    
//    val total =arrayRows.length
//    
//    val pageSize=50000;
//    
//    val totalPage=if(total/pageSize==0)total/pageSize else (total/pageSize)+1
//    
//    for(i<-0 until totalPage){
//      val tmpList=lists.subList(i*pageSize, if((i+1)*pageSize>total)total else (i+1)*pageSize)
//      
//      val sql=buildInsertSql(tmpList)
//    }
    arrayRows
  }
  
  def getHiveMaxRowNum(appName: String,bean:Hive2DbmsBean,sc:SparkContext)={
    HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
    
    val arrayRows=HiveContextUtil.execQuery(sc, "select count(1) from ".concat(bean.getHiveTableName))
    
    if(null==arrayRows || arrayRows.length==0)
      null
     
    arrayRows.apply(0).get(0).toString()
  }
  
  def exportHaNaTable(appName: String,bean:Hive2DbmsBean,sc:SparkContext,lowerBound:Long,upperBound:Long){
    HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
    
//    val arrayRows=HiveContextUtil.execQuery(sc, "select max(rownumber) from ".concat(bean.getHiveTableName))
//    
//    if(null==arrayRows || arrayRows.length==0)
//      return
//    
//    val rowNum=arrayRows.apply(0).get(0).toString()
    
    val sqlContext = new SQLContext(sc)
    
    val prop = new Properties()
    prop.put("user", bean.getRdbmsJdbcUser)
    prop.put("password", bean.getRdbmsJdbcPasswd)
    
    var df=sqlContext.read.jdbc(bean.getRdbmsJdbcUrl, bean.getRdbmsSchemaName+".\""+bean.getRdbmsTableName+"\"", "rownumber", lowerBound, upperBound, 100, prop)
    
//    if(df.rdd.isEmpty())
//      return
    
    df.createOrReplaceTempView(bean.getRdbmsTableName)
    
    df=df.sqlContext.sql("select * from ".concat(bean.getRdbmsTableName).concat(" where rownumber between ")
        .concat(String.valueOf(lowerBound)).concat(" and ").concat(String.valueOf(upperBound)))
    
        
    val fileName = bean.getHdfsUrl + "/tmp/mysqlhive/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/" + lowerBound + "_" + upperBound
    df.rdd.map { r =>
      {
        val s: StringBuilder = new StringBuilder("")
        for (j <- Range(0, r.length)) {
          var o = r.get(j)
          if (o == null || o.toString().trim().equals(""))
            o = "NULL";
          if (o.isInstanceOf[String]) {
            if ("null".equals(o.asInstanceOf[String].toLowerCase()))
              o = "NULL";
            o = (o.asInstanceOf[String]).replaceAll("\n", " ");
            o = (o.asInstanceOf[String]).replaceAll("\t", " ");
          }
          s.append(o);
          if (j < r.length - 1)
            s.append("\t")
        }
        s
      }
    }.saveAsTextFile(fileName)
    
    df.rdd.cache()
    
    var hdfs:FileSystem=null
    
    val mysql=new MysqlSpark()
    
    hdfs=new SparkHdfsUtil().getFileSystem(bean.getHdfsUrl)
    val destFileName = bean.getHdfsUrl + "/tmp/hanaGen/" + bean.getHiveSchema + "/" + bean.getHiveTableName + "/" + lowerBound + "_" + upperBound+".txt"
    mysql.mergeHdfsFile(hdfs, destFileName, fileName, bean.getHdfsUrl)
  }
  
  def handlerLoadHaNaWithJdbc(appName: String,bean:Hive2DbmsBean,sc:SparkContext,dataSourcePath:String){
    HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
    
    val df=HiveContextUtil.getRDD(sc, bean.getHiveSql)
    
    if(null==bean.getPartition || 0==bean.getPartition)
      bean.setPartition(200)
    
    df.rdd.repartition(bean.getPartition).foreachPartition { rows => {
        object InternalSparkJdbcUtil extends SparkJdbcTemplete {}
        
        InternalSparkJdbcUtil.instance.buildJdbcTemplete(dataSourcePath)
        
        val whole =new StringBuilder("")
        var sb:StringBuilder=null
        while(rows.hasNext){
          val r=rows.next()
          
          sb=new StringBuilder("upsert ")
          sb.append(bean.getRdbmsSchemaName).append(".").append(bean.getRdbmsTableName).append(" values (")
          var primaryKey:String=null
          var tempString:String=null;
          for (j <- Range(0, r.length)) {
            if(j==0){
              primaryKey=r.get(j).toString().trim()
              
              if("NULL".equals(primaryKey) || null==primaryKey || "".equals(primaryKey)){
                //主键为空时不添加任何sql语句
                primaryKey="NULL"
              }else{
                sb.append("'").append(primaryKey).append("'")
              }
            }else{
              tempString=r.get(j).toString().trim()
              
              if("NULL".equals(tempString) || null==tempString || "".equals(tempString)){
                sb.append(",null")
              }else{
                sb.append(",'").append(tempString).append("'")
              }
            }
          }
          sb.append(") where ").append(bean.getPrimaryKey).append("=").append("'").append(primaryKey).append("';")
          
         if(!"NULL".equals(primaryKey)){
           whole.append(sb)
         }
        }
        
        if(whole.length>0){
          val st=InternalSparkJdbcUtil.instance.getSt()
          
          val temp=whole.toString().split(";")
          
          var k=0
          val size:Int=if(null==bean.getTotalOnce)50000 else bean.getTotalOnce.intValue()
          
          while(k<=temp.length){
             val end=if(k+size>temp.length) temp.length else k+size
             if(end-k>0){
               val subArray:Array[String]=new Array[String](end-k)
               Array.copy(temp,k,subArray,0,end-k)
               
               val handler=new HaNaJdbcHandler(subArray)
               
               st.doIntrans(handler)
             }
             k+=size
          }
//            InternalSparkJdbcUtil.instance.exec(whole.toString())
       }

        
    } }
  }
  
  def genCsvForHaNaWithSql(appName: String,bean:Hive2DbmsBean,sc:SparkContext){
    HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
    
    val df=HiveContextUtil.getRDD(sc, bean.getHiveSql)
    
    val url=bean.getHdfsUrl
    
    val fileName = url + "/tmp/hana/" + bean.getHiveSchema + "/" + bean.getHiveTableName
    
    val splitKey:String="\t";
    
    df.rdd.repartition(100).map { r => {
        val sb:StringBuilder=new StringBuilder()
        for (j <- Range(0, r.length)) {
          var o=r.get(j)
          
          if(o.isInstanceOf[String]){
            if(!o.asInstanceOf[String].equals("NULL")){
              if(0==j){
                sb.append("\"").append(r.get(j).asInstanceOf[String].trim()).append("\"")
              }else{
                sb.append(splitKey).append("\"").append(r.get(j).asInstanceOf[String].trim()).append("\"")
              }
            }else{
              if(0==j){
                sb.append("\"").append("\"")
              }else{
                sb.append(splitKey).append("\"").append("\"")
              }
            }
          }else{
            if(0==j){
              sb.append("\"").append(String.valueOf(o).trim()).append("\"")
            }else{
              sb.append(splitKey).append("\"").append(String.valueOf(o).trim()).append("\"")
            }
          }
          
        }
        sb
     } }.saveAsTextFile(fileName)
     
    var hdfs:FileSystem=null
    hdfs=new SparkHdfsUtil().getFileSystem(bean.getHdfsUrl)
    val mysql=new MysqlSpark()
    mysql.mergeHdfsFile(hdfs, "/tmp/hana/" + bean.getHiveSchema + "/" + bean.getHiveTableName+".csv", fileName, bean.getHdfsUrl)
  }
  
  def genCsvForHaNaWithHivePath(appName: String,bean:Hive2DbmsBean,sc:SparkContext){
    val url=bean.getHdfsUrl
    
    val conf = new Configuration()
    
    val hdfs=FileSystem.get(URI.create(url),conf)
    
    var path:String=null;
    
    val fileName = url + "/user/hive/warehouse/" + bean.getHiveSchema + "/" + bean.getHiveTableName
    //hdfs.delete(new Path(fileName), true)
    val splitKey:String="\t";
    
    if(hdfs.isDirectory(new Path(bean.getHiveUrl))){
     // val rdd=sc.textFile(bean.getHdfsUrl+bean.getHiveUrl+"/*", 100)
      path=bean.getHdfsUrl+bean.getHiveUrl+"/*"
    }else{
      path=bean.getHdfsUrl+bean.getHiveUrl
    }
    
     val rdd=sc.textFile(path, 100)
     
     rdd.repartition(100).map { r => {
        val sb:StringBuilder=new StringBuilder()
          
          val arr=r.split("\t")
          
          for(i<-0 until arr.length){
            var temp:String=""
            if(!arr(i).equals("asnull") && !arr(i).equals("NULL")){
                temp=arr(i)
               if(i==0){
                sb.append("\"").append(temp.trim()).append("\"")
               }else{
                 sb.append(splitKey).append("\"").append(temp.trim()).append("\"")
                  } }
            else if(arr(i).equals("asnull") ||arr(i).equals("NULL") ) {
              if(i==0){
              sb.append("\"").append("\"")
            }else{
              sb.append(splitKey).append("\"").append("\"")
              }
            }
          }
          sb
     } }.saveAsTextFile(fileName)
     
     val mysql=new MysqlSpark()
     val destFileName = "/tmp/hana/" + bean.getHiveSchema + "/" + bean.getHiveTableName+".csv"
     mysql.mergeHdfsFile(hdfs,destFileName, fileName, bean.getHdfsUrl)
     
     //导出HDFS文件到本地
     val dest = bean.getHdfsUrl + destFileName
     val local = "/tmp/hana/utags/zhy_tag.csv"
     val fs = FileSystem.get(URI.create(dest),conf)
     //fs.copyToLocalFile(false,new Path(destFileName), new Path(local),true)
     val fsdi : InputStream = fs.open(new Path(dest))
     val output : OutputStream = new BufferedOutputStream(new FileOutputStream(new File(local)))
     IOUtils.copyBytes(fsdi,output,4096,true)
     
  }
  
}