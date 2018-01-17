package com.lppz.spark.oms.scala;

import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.ArrayList
import java.util.Arrays.ArrayList
import java.util.Date
import java.util.HashMap
import java.util.HashSet
import java.util.Map
import java.util.UUID

import org.apache.log4j.Logger
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import com.lppz.oms.api.model.HistOrderQueryParams
import com.lppz.spark.oms.utils.ExcelUtil
import com.lppz.spark.oms.utils.ExportExcelFileUtil
import com.lppz.spark.oms.utils.RestClientUtil
import com.lppz.spark.oms.utils.ReturnResonUtils
import com.lppz.spark.oms.utils.SqlUtil
import com.lppz.spark.scala.HiveContextUtil
import com.lppz.spark.scala.SparkHdfsUtil
import com.lppz.spark.scala.jdbc.MysqlSpark
import com.lppz.spark.oms.utils.CacheUtil
import org.apache.spark.HashPartitioner
import java.io.FileSystem
import org.apache.hadoop.fs.Path

class ExportOmsHisDataHandler extends Serializable {
  @transient lazy val LOG = Logger.getLogger(getClass.getName)

  def exportOmsData(appName: String, bean: HistOrderQueryParams, schema: String, hdfsUrl: String
      , sc: SparkContext,mode:String, mysqlUrl:String,userName:String,pwd:String,pathName:String) = {
    HiveContextUtil.exec(sc, "use " + schema)
    var patition = 10;
    var dataSql = "";
    var fileLines = 50000;
    if (bean.getPageSize !=0) {
    	fileLines = bean.getPageSize
    }
    val queryType = bean.getQueryType.getType
    		var ordetType = ""
    		var orderCategory = ""
    		if (bean.getOrderType != null) {
    			ordetType = bean.getOrderType.getCode
    		}
    
    if (bean.getOrderCategoryType != null) {
    	orderCategory=bean.getOrderCategoryType.getId
    }
    if (bean.getQueryType.getType == 1) {
      dataSql = SqlUtil.buildOrderStatistics_SQL(bean);
    } else if (bean.getQueryType.getType == 2) {
      dataSql = SqlUtil.buildReturnOrderPaged_SQL(bean)
    } else if (bean.getQueryType.getType == 3) {
      dataSql = SqlUtil.buildCancelOrder_SQL(bean)
    }
    val dataframe = HiveContextUtil.getRDD(sc, dataSql)
    val columns = dataframe.columns
    val rdd = dataframe.rdd
    rdd.cache()
    val totalresultNum = rdd.count();
    val bastPath = pathName
    val sortByColumn = bean.getSortByColumn;
    patition = (totalresultNum.asInstanceOf[Int] - 1) / fileLines + 1
    LOG.debug("totalResultNum : "+totalresultNum + " fileLines :  " + fileLines +" patitionNum : "+ patition+"sql : " + dataSql)
    val rr=rdd.map { row => (row.get(0).asInstanceOf[String],row) }
    var rp:RDD[(String,Row)] = null;
    if (sortByColumn !=""&& sortByColumn!=null) {
      rp = rr.partitionBy(new HashPartitioner(patition)).sortBy(m=> m._2.getString(columns.indexOf(sortByColumn)))
    }else{
      rp = rr.partitionBy(new HashPartitioner(patition));
    }
    rp.foreachPartition { iter =>
      {
    	  var util:ExportExcelFileUtil = null
    	  if(queryType==1){
    		  val cacheUtil = new CacheUtil(mysqlUrl,userName,pwd)
    		  val cacheMap = cacheUtil.getCacheMap;
    		  util = new ExportExcelFileUtil(cacheMap)
    	  }else{
    	    util = new ExportExcelFileUtil()
    	  }

        var resultList = new ArrayList[Map[String, String]]()
        iter.foreach { r =>
          val row=r._2
          val size = row.size
          val rowMap = new HashMap[String, String]
          for (i <- 0 to size-1) {
            rowMap.put(columns(i), row.getString(i))
          }
          resultList.add(rowMap)
        }
        if (resultList.size() > 0) {
        	exportExecuter(util, resultList, queryType, ordetType, orderCategory,hdfsUrl,bastPath,fileLines)
        }
      }
    }
  }
  
  def writeOverFile(hdfsUrl:String, overMark:String){
	  val hdfs = new SparkHdfsUtil().getFileSystem(hdfsUrl);
//	  hdfs.create(new Path(overMark));
	  hdfs.createNewFile(new Path(overMark));
  }
  
  def exportExecuter(util:ExportExcelFileUtil,resultList:ArrayList[Map[String,String]]
  ,queryType:Int,orderType:String,orderCategory:String,hdfsUrl:String,basehdfsFilePath:String,lineNum:Int):String={
    LOG.debug("start to buildExcel file size " + resultList.size())
	  val hdfsFileStr = basehdfsFilePath+"/"+queryType+UUID.randomUUID().toString() +".xls"
	  var hssWb:HSSFWorkbook = null
	  if (queryType == 1) {
		  hssWb = exportOrderStaticData(util, resultList, orderType, orderCategory)
	  } else if (queryType == 2) {
		  hssWb = exportReturnData(util, resultList)
	  } else if (queryType == 3) {
		  hssWb = util.exportCancelExcel(resultList)
	  }
    ExcelUtil.saveFileToHdfs(hssWb,hdfsFileStr, hdfsUrl)
    return hdfsFileStr
  }

  def exportOrderStaticData(util: ExportExcelFileUtil , resultList: ArrayList[Map[String, String]],
      orderType: String, orderCategoryType: String):HSSFWorkbook = {
    val orderresultList = new ArrayList[Map[String, String]]()
    val orderlineResultList = new ArrayList[Map[String, String]]()
    val orderidSet = new HashSet[String]
    val orderLineidSet = new HashSet[String]
    if (resultList != null) {
      for (i <- 0 to resultList.size()-1) {
        val id = resultList.get(i).get("orderHybrisId")
        val lineid = resultList.get(i).get("orderlineid")
        if(!orderidSet.contains(id)){
          orderidSet.add(id)
          orderresultList.add(resultList.get(i))
        }
        if(!orderLineidSet.contains(lineid)){
        	orderLineidSet.add(lineid)
        	orderlineResultList.add(resultList.get(i))
        }
      }
      return util.exportOrderstaticExcel(orderresultList, orderlineResultList, orderType, orderCategoryType)
    }
    return null;
  }

  def exportReturnData(util: ExportExcelFileUtil, resultList: ArrayList[Map[String, String]]):HSSFWorkbook = {
    val returnOnly = new HashMap[String, String]();
    val returnsList = new ArrayList[Map[String, String]]
    var map:Map[String,String] = null;
    if (resultList != null) {
      for (i <- 0 to resultList.size()-1) {
        map = resultList.get(i);
        val returnhbid = map.get("returnhbid")
        var reasonNames = "";
        if (returnOnly.get(returnhbid) != null) {
        	val reason = ReturnResonUtils.getReason(map.get("reasontype"));
        	reasonNames = returnOnly.get(returnhbid) + "/" + reason;
        } else {
        	reasonNames = ReturnResonUtils.getReason(map.get("reasontype"));
        	returnsList.add(map)
        }
        returnOnly.put(returnhbid, reasonNames);
      }
    }
    return util.exportReturnExcel(returnsList, returnOnly);
  }
}