package com.lppz.spark.accmember.scala

import java.util.HashMap
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import com.lppz.spark.accmember.bean.AccExportBean
import com.lppz.spark.accmember.bean.TableBean
import com.lppz.spark.scala.jedis.JedisClientUtil
import java.math.BigDecimal

class AccCalcMemberScoreHandler extends Serializable {
  @transient lazy val LOG = Logger.getLogger(getClass.getName)

  /**
   * 需求：
   * 以前的会员记录表，一个人有多个会员卡，每个会员卡积分和储值各是一条记录，
   * 现在的会员中台设计的是一个人一张表，把同一个人的所有积分和储值值合并记录
   * 方案：
   * ①查询10.8.202.231上acc的lpmemberscores表3个字段p_lppzmembers、p_membertypecd、p_memberscore
   *  select p_lppzmembers as PK, p_membertypecd as type ,p_memberscore from lpmemberscores  a  limit 1000;
   *
   * 其中8796107767899的code为LPPZ_PT是积分值，8796107833435的code为LPPZ_CZ是储蓄值
   *  select `Code` ,pk from enumerationvalues where PK in('8796107767899','8796107833435');
   * ②合并会员值相同的积分和储值记录的总和，即group by p_lppzmembers ,sum('LPPZ_PT'),sum('LPPZ_CZ')
   * ③存在redisCluster中,key=pk,value={sum('LPPZ_PT'):p_memberscore,sum('LPPZ_CZ')p_memberscore}
   */
  def startExportToRedisCuster(sc: SparkContext, bean: AccExportBean, table: TableBean, jedisClusterYamlPath: String, lowerBound: Long, upperBound: Long) {
    val sourceDriver = bean.getSourceDriver
    val sourceUrl = bean.getSourceJdbcUrl
    val sourceUser = bean.getSourceUser
    val sourcePwd = bean.getSourcePwd
    val tables = bean.getTables

    val sqlContext = new SQLContext(sc)

    val prop = new Properties()
    prop.put("user", bean.getSourceUser)
    prop.put("password", bean.getSourcePwd)

    var df = sqlContext.read.jdbc(sourceUrl, table.getSourceTableOrView, table.getPrimaryKey, lowerBound, upperBound, bean.getPartition, prop)
    df.createOrReplaceTempView(table.getSourceTableOrView)
    //拼接sql
    df = df.sqlContext.sql("select p_lppzmembers as pk, p_membertypecd as type ,p_memberscore as score from "
      .concat(table.getSourceTableOrView.concat(" where ").concat(table.getPrimaryKey).concat(" between ").concat(String.valueOf(lowerBound))
        .concat(" and ").concat(String.valueOf(upperBound))))

    LOG.info("================创建df完成==========");
    //计算同一用户积分和储值的累积值
    LOG.info("================开始循环遍历rdd中row,计算同一用户积分和储值的累积值==========");

    df.rdd.repartition(table.getPartition).foreachPartition { it =>
      {
        object InternalRedisClient extends JedisClientUtil {}
        InternalRedisClient.instance.makeJedisCluster(jedisClusterYamlPath)
        val jedisCluster = InternalRedisClient.instance.getJedisCluster

        it.foreach { row =>
          {
            val pk = row.get(row.fieldIndex("pk")).toString()

            //获取积分类型值和积分对应的值
            val p_membertypecd = row.get(row.fieldIndex("type")).toString()
            //将值转化为Double类型
            val p_memberscore: Double = row.getDouble(row.fieldIndex("score"))
            if (null == p_memberscore) {
              jedisCluster.hset(pk.getBytes, p_membertypecd.getBytes, "0".getBytes)
            } else {
              jedisCluster.hincrByFloat(pk.getBytes, p_membertypecd.getBytes, p_memberscore)
            }
          }
        }
      }
    }
  }
}