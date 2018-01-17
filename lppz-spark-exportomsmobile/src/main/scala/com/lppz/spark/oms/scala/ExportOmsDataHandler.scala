package com.lppz.spark.sap.scala;

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.lppz.spark.scala.HiveContextUtil
import com.lppz.spark.oms.utils.BuildLoadFileUtil
import org.apache.spark.sql.Row
import com.lppz.spark.scala.jdbc.MysqlSpark
import org.apache.hadoop.fs.FileSystem
import com.lppz.spark.scala.SparkHdfsUtil
import com.lppz.spark.oms.bean.ExportBean
import org.apache.commons.lang3.StringUtils

class ExportOmsDataHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName) 
  
    def exportOmsMoblie(appName: String,bean:ExportBean,sc:SparkContext,isFirstLoad:Boolean,ds:String,lowerBound:Long,upperBound:Long)={
      val util = new BuildLoadFileUtil
      HiveContextUtil.exec(sc, "use " + bean.getSchema)
      var fileName=""
      var destFileName = ""
      if(ds != null){
        fileName = "/tmp/omstmallmember/"+ds+".txt"
        destFileName =bean.getHdfsUrl + "/tmp/omstmallmemberresult/"+ds+".txt"
      }else{
         fileName = "/tmp/omstmallmember/"+lowerBound + "_" + upperBound+".txt"
         destFileName =bean.getHdfsUrl + "/tmp/omstmallmemberresult/"+lowerBound + "_" + upperBound+".txt"
      }
      var resultList = List[String]()
      var executesql = ""
      if (isFirstLoad) {
    	  executesql = bean.getDataSql.concat(" and s.id between ").concat(String.valueOf(lowerBound)).concat(" and ").concat(String.valueOf(upperBound))
      }else{
        executesql = bean.getDataSql.replaceAll("ds-", ds)
      }
       val dataframe = HiveContextUtil.getRDD(sc, executesql)
            dataframe.rdd.filter { r => StringUtils.isNotBlank(r.get(0).asInstanceOf[String])}.repartition(bean.getPartition).map { row => 
    		    val sql = util.buildLoadFile(row)
    				  sql
    	  }.saveAsTextFile(fileName)
    	 
    	  val mysql=new MysqlSpark()
        
        var hdfs:FileSystem=null
      
        hdfs=new SparkHdfsUtil().getFileSystem(bean.getHdfsUrl)
      
        mysql.mergeHdfsFile(hdfs, destFileName, fileName, bean.getHdfsUrl) 
      
  }
  
  def getMaxOmsOrderShardingId(appName: String,schema:String,sql:String,sc:SparkContext)={
       HiveContextUtil.exec(sc, "use " + schema)
       val maxId = HiveContextUtil.execQuery(sc, sql).apply(0).getInt(0);
       maxId
  }
                  
}