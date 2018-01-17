package com.lppz.spark.scala.bean

import org.apache.spark.SparkContext
import com.lppz.spark.bean.SparkSqlDmlBean
import org.apache.spark.sql.DataFrame
import com.lppz.spark.scala.HiveContextUtil
import scala.beans.BeanProperty
import scala.util.control.Breaks
import com.lppz.spark.scala.hbase.HbaseHandler
import com.lppz.spark.scala.jedis.JedisClientUtil
import redis.clients.jedis.Jedis
import com.lppz.spark.scala.hbase.HbaseAbstractSparkConv
import com.lppz.spark.scala.hbase.HbaseSparkConvertor
import java.util.HashSet

class Spark2HbaseBean(@BeanProperty var sqlBean: SparkSqlDmlBean, 
                      @BeanProperty var targetBean: HBaseBean, @BeanProperty var sparkBeanMap: java.util.Map[String, Spark2HbaseBean]) extends Serializable {
  def this() = this(null, null, null)

  def buildSparkSqlRdd(sc: SparkContext,sqlStr: String): DataFrame = {
    HiveContextUtil.exec(sc, "use " + sqlBean.getSchema())
    HiveContextUtil.getRDD(sc, sqlStr)
  }

  def loopExec(sc: SparkContext,jedisClusterYamlPath: String,jedisSentinelPath:String,month: String,batchNum:Integer,pageNum:Long,partitionNum:Integer,paramMap:java.util.Map[String,String],transCoder:(java.util.HashMap[String,String],java.util.Map[String,String])=> java.util.HashMap[String,String]) {
    var i: Long = 1
    if(pageNum>0)
      sqlBean.setTotal4Once(pageNum)
    if(partitionNum>0)
    	 targetBean.setPartionNum(partitionNum)
    
    val importTables = paramMap.get(HbaseSparkConvertor.importTableKey)
    val importTableList  = new HashSet[String]
    
    if (importTables != null) {
    	importTables.split(",").foreach { x => importTableList.add(x)}
    }
    
    if (importTableList.contains(sqlBean.getFamilyName)) {
      val loop = new Breaks;
      loop.breakable {
    	  while (true) {
    		  val sqlStr = sqlBean.getSql.replace("#month#", month).replace("#start#", String.valueOf(i))
    				  .replace("#end#", String.valueOf(i + sqlBean.getTotal4Once))
    				  i += sqlBean.getTotal4Once + 1
    				  val df = buildSparkSqlRdd(sc,sqlStr).repartition(partitionNum)
    				  if (df != null && df.rdd != null && (!df.rdd.isEmpty())) {
    					  object InternalHBaseCli extends HbaseHandler {}
    					  object InternalRedisClient extends JedisClientUtil {
    					  }
    					  InternalRedisClient.instance.makeJedis(jedisSentinelPath)
    					  val jedis:Jedis = InternalRedisClient.instance.getJedisProxy
    					  val convertor:HbaseAbstractSparkConv = sqlBean.buildConverter(targetBean.getHbaseQuorum(),targetBean.getHbasePort(),jedis)
    					  convertor.setParamMap(paramMap)
    					  InternalHBaseCli.instance.saveRddRow2HBase(df,sqlBean.getExcludeColumnList, convertor, targetBean.getHbaseQuorum(), targetBean.getHbasePort(), targetBean.getTablebname(), targetBean.getCf(), targetBean.getPartionNum(), targetBean.isWal, jedisClusterYamlPath,jedisSentinelPath,batchNum,transCoder)
    				  } else {
    					  loop.break
    				  }
    	  }
      }
    }

    if (sparkBeanMap != null && (!sparkBeanMap.isEmpty())) {
      val it = sparkBeanMap.values().iterator();
      while (it.hasNext()) {
        val sbean = it.next()
//        if (importTableList.contains(sbean.getSqlBean().getFamilyName)) {
        	sbean.loopExec(sc,jedisClusterYamlPath,jedisSentinelPath,month,batchNum,pageNum,partitionNum,paramMap,transCoder)
//        }
      }
    }
  }
}