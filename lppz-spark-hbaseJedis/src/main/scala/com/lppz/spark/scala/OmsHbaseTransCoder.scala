package com.lppz.spark.scala

import com.lppz.spark.util.OmsBaseStoreUtil
import scala.beans.BeanProperty
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.sys.process._
import com.lppz.spark.scala.hbase.HbaseSparkConvertor


/**
 * @author zoubin
 */
class OmsHbaseTransCoder() extends AbstractHbaseTransCoder{
  def buildParamMap():java.util.Map[String,String]={
//    val filePath:String = "source /etc/profile;echo ${OmsJdbcFilePath}" !!
//    val pwdPath:String = "pwd" !!
//    ProcessLogger(pwdPath=>{
//    	println(pwdPath)
//    })
//    val path = pwdPath.replace("\n", "")+"/jdbc.properties";
//    OmsBaseStoreUtil.loadResource(path)
    val map = OmsBaseStoreUtil.getAllIdCode
    map.put(HbaseSparkConvertor.doTransferTableKey, OmsBaseStoreUtil.getKey(HbaseSparkConvertor.doTransferTableKey));
    map.put(HbaseSparkConvertor.importTableKey, OmsBaseStoreUtil.getKey(HbaseSparkConvertor.importTableKey));
    map
  }
  
  
 override def getTransferCoder(): ((java.util.HashMap[String,String],java.util.Map[String,String]) => java.util.HashMap[String,String]) ={
    val transCoder=(rowmap:java.util.HashMap[String,String],paraMap:java.util.Map[String,String])=> {
       if(rowmap.containsKey("basestore")){
         val id = rowmap.get("basestore")
         val code = paraMap.get(id)
         if(code !=null){
        	 rowmap.put("basestore", code)
         }
       }
       rowmap
    }
    super.setTransferCoder(transCoder)
    return transCoder
  }
}