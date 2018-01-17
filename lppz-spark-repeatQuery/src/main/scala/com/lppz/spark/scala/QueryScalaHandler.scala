package com.lppz.spark.scala

import com.lppz.spark.scala.jedis.JedisClientUtil
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, Dataset}

/**
 * @author chenlisong
 */
class QueryScalaHandler extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName)
  def exec(sc: SparkContext, sqlStr: String,jedisClusterYamlPath: String) {
    //val d = HiveContextUtil.getRDD(sc, sqlStr)

    val d = sc.textFile("/opt/abc.txt");

    //d.show(10);

    val colArray: Array[String] = new Array[String](328)

    d.foreachPartition { partitionOfRecords =>
      val index: Int = partitionOfRecords.indexOf() + 1

      var col: StringBuffer = new StringBuffer()
      col.append("orderid in (")

      partitionOfRecords.foreach(record => {
        col.append("'"+record+"'")
      }
      )
      col.append(")")
      colArray.update(index, col.toString);
    }
    LOG.info("colArray content:" + colArray(0));

  }
}