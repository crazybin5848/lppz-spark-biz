package com.lppz.spark.accmember.scala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import com.lppz.spark.accmember.bean.AccExportBean
import com.lppz.spark.accmember.bean.TableBean
import org.apache.spark.sql.SQLContext
import java.util.Properties
import com.lppz.spark.scala.jdbc.SparkJdbcTemplete
import java.util.ArrayList
import org.apache.spark.sql.Row
import com.lppz.spark.bean.jdbc.JdbcDBTemplate
import java.util.HashMap
import com.lppz.spark.scala.jdbc.JdbcSqlHandler
import org.springframework.jdbc.UncategorizedSQLException
import scala.util.control.Breaks
import org.apache.hadoop.fs.FileSystem
import com.lppz.spark.scala.jdbc.MysqlSpark
import com.lppz.spark.scala.SparkHdfsUtil
import com.lppz.spark.scala.jedis.JedisClientUtil
import com.lppz.spark.util.jedis.SparkJedisCluster

class AccExportHandler extends Serializable {
  @transient lazy val LOG = Logger.getLogger(getClass.getName)

  def startExportWithView(sc: SparkContext, bean: AccExportBean, table: TableBean, dataSourcePath: String, lowerBound: Long, upperBound: Long, batch: Boolean, enumTable: HashMap[String, String]) {
    val sourceDriver = bean.getSourceDriver
    val sourceUrl = bean.getSourceJdbcUrl
    val sourceUser = bean.getSourceUser
    val sourcePwd = bean.getSourcePwd
    val tables = bean.getTables

    //初始化sql上下文
    val sqlContext = new SQLContext(sc)

    val prop = new Properties()
    prop.put("user", bean.getSourceUser)
    prop.put("password", bean.getSourcePwd)
    //初始化数据库操作对象dataframe
    var df = sqlContext.read.jdbc(sourceUrl, table.getSourceTableOrView, table.getPrimaryKey, lowerBound, upperBound, bean.getPartition, prop)

    //创建或更新视图
    df.createOrReplaceTempView(table.getSourceTableOrView)
    //组装执行sql，并设置到sql上下文中
    df = df.sqlContext.sql("select * from ".concat(table.getSourceTableOrView.concat(" where ")
      .concat(table.getPrimaryKey).concat(" between ").concat(String.valueOf(lowerBound)).concat(" and ").concat(String.valueOf(upperBound))))
    var batchSize = table.getBatchSize
    //执行sql并把才查询结果分partition处理
    df.rdd.repartition(table.getPartition).foreachPartition { it =>
      val sqlList = new ArrayList[String]()
      //scala声明一个jdbc工具类
      object InternalSparkJdbcUtil extends SparkJdbcTemplete {}

      //初始化jdbc工具类
      InternalSparkJdbcUtil.instance.buildJdbcTemplete(dataSourcePath)
      val st = InternalSparkJdbcUtil.instance.getSt()
      
      //遍历partition中的行
      it.foreach { row =>
        //根据行数据组装成需要的sql
        var sql = buildInsertSql(table, enumTable, row)
        LOG.debug("++++++++" + sql)
        sqlList.addAll(sql)
        //根据设置的批量插入大小，批量执行插入操作
        if (sqlList.size() >= batchSize && !batch) {
          batchInsert(sqlList, st);
        }
      }
      if (!sqlList.isEmpty() && !batch) {
        LOG.info("not empty size " + sqlList.size())
        batchInsert(sqlList, st);
      }
      //一次性批量插入数据
      if (batch && !sqlList.isEmpty()) {
        InternalSparkJdbcUtil.instance.mulitExec(sqlList, batchSize)
      }
      st.close()
    }
  }

  def startExportGenFile(sc: SparkContext, bean: AccExportBean, table: TableBean, dataSourcePath: String, jedisClusterYamlPath: String, lowerBound: Long, upperBound: Long, batch: Boolean, enumTable: HashMap[String, String]) {
    val sourceDriver = bean.getSourceDriver
    val sourceUrl = bean.getSourceJdbcUrl
    val sourceUser = bean.getSourceUser
    val sourcePwd = bean.getSourcePwd
    val tables = bean.getTables
    
    //初始化sql上下文
    val sqlContext = new SQLContext(sc)

    val prop = new Properties()
    prop.put("user", bean.getSourceUser)
    prop.put("password", bean.getSourcePwd)

    //初始化数据库操作对象dataframe
    var df = sqlContext.read.jdbc(sourceUrl, table.getSourceTableOrView, table.getPrimaryKey, lowerBound, upperBound, bean.getPartition, prop)
     //创建或更新视图
    df.createOrReplaceTempView(table.getSourceTableOrView)
     //组装执行sql，并设置到sql上下文中
    df = df.sqlContext.sql("select * from ".concat(table.getSourceTableOrView.concat(" where ")
      .concat(table.getPrimaryKey).concat(" between ").concat(String.valueOf(lowerBound)).concat(" and ").concat(String.valueOf(upperBound))))

      //执行sql并缓存到内存
    if(table.getTargetTableName.size>1){
      df.rdd.cache()
    }
    //遍历
    for (k <- Range(0, table.getTargetTableName.size)) {
      val targetTableName = table.getTargetTableName.get(k)
      //定义hdfs文件名和存放路径
      val fileName = bean.getHdfsUrl + "/tmp/acc/" + targetTableName + "/" + lowerBound + "_" + upperBound
      if (table.getRedisKey != null && !table.getRedisKey.equals("")) {
        //把结果数据转换为key-row
        val bb=df.rdd.keyBy { row => {
//          row.get(row.fieldIndex(table.getPrimaryKey)).toString()
          row.get(row.fieldIndex("member_no")).toString()
        } }
        def aa(f: (String, Iterable[Row]))={
          f._2.iterator.next()
        }
        
        //数据根据key分组，相同的key放到同一个partition中处理
        bb.groupByKey().map(aa).repartition(table.getPartition).mapPartitions(it => {
          var result = List[String]()
          //声明jediscluster工具类
          object InternalRedisClient extends JedisClientUtil {}
          InternalRedisClient.instance.makeJedisCluster(jedisClusterYamlPath)
          val jedisCluster = InternalRedisClient.instance.getJedisCluster
          //声明结果集处理工具类
          object InternalBuildFileUtil extends BuildLoadFileHandler {}
           //遍历结果集，每行数据生成一行文本
          while (it.hasNext) {
            val row = it.next
            val sql = InternalBuildFileUtil.instance.handler(targetTableName, table, enumTable, row, jedisCluster)
            result = result.::(sql)
          }
          jedisCluster.close()
          result.iterator
          //集合写到hdfs上
        }, true).saveAsTextFile(fileName)
      } else {
        df.rdd.map { row =>
          {
            object InternalBuildFileUtil extends BuildLoadFileHandler {}
            InternalBuildFileUtil.instance.handler(targetTableName, table, enumTable, row, null)
          }
        }.saveAsTextFile(fileName)
      }

      //合并文本文件到指定目录
      var hdfs: FileSystem = null
      val mysql = new MysqlSpark()
      hdfs = new SparkHdfsUtil().getFileSystem(bean.getHdfsUrl)
      val destFileName = bean.getHdfsUrl + "/tmp/accGen/" + targetTableName + "/" + lowerBound + "_" + upperBound + ".txt"
      mysql.mergeHdfsFile(hdfs, destFileName, fileName, bean.getHdfsUrl)
    }
  }
  

  def buildInsertSqlNew(tableName: String, table: TableBean, enumTable: HashMap[String, String], r: Row) = {
    val tableNames = table.getTargetTableName
    val joinKey = table.getJoinKey
    val selectColumns = table.getSelectColumn
    val jk = joinKey.get(tableName)
    val selectColumn = selectColumns.get(tableName)
    val nullType = "\\N"
    val splitKey = "\t"
    var sb = new StringBuilder();
    for (j <- Range(0, selectColumn.split(",").length)) {
      var x = selectColumn.split(",")(j)
      if (x.startsWith("#")) {
        var column = x.replaceFirst("#", "")
        var index = r.fieldIndex(column)
        var o = r.get(index)

        if (o == null || o.toString().equals("")) {
          sb.append(nullType)
        } else {
          var cc = enumTable.get(o)
          sb.append(if (cc == null) nullType else cc)
        }
      } else {
        var index = r.fieldIndex(x)
        var o = r.get(index)
        if (o == null)
          o = nullType;
        if (o.isInstanceOf[String]) {
          if ("null".equals(o.asInstanceOf[String].toLowerCase()))
            o = nullType
          o = (o.asInstanceOf[String]).replaceAll("\r", "")
          o = (o.asInstanceOf[String]).replaceAll("\n", "")
          o = (o.asInstanceOf[String]).replaceAll("\t", " ")
        }

        sb.append(o)
      }

      if (j < selectColumn.split(",").length - 1)
        sb.append(splitKey)
    }
    sb.append("\n")
    sb.toString()
  }

  def buildInsertSql(table: TableBean, enumTable: HashMap[String, String], r: Row) = {
    val tableNames = table.getTargetTableName
    val excludePrimaryColumn = table.getPrimaryKey
    val joinKey = table.getJoinKey
    val targetColumns = table.getTargetColumns
    val selectColumns = table.getSelectColumn

    val rtnList = new ArrayList[String]
    for (k <- Range(0, tableNames.size)) {
      val tableName = tableNames.get(k)

      val valueMap = new HashMap[String, String]
      val jk = joinKey.get(tableName)

      for (i <- Range(0, jk.size())) {
        val column = jk.get(i)

        val rIndex = r.fieldIndex(jk.get(i))

        var o = r.get(rIndex)

        if (o == null || "".equals(o.toString())) {
          valueMap.put(column, null)
        } else {
          valueMap.put(column, enumTable.get(o.toString()))
        }
      }

      val sql: StringBuilder = new StringBuilder("INSERT INTO ")
      sql.append(tableName).append("(").append(targetColumns.get(k)).append(") values(")

      val selectColumn = selectColumns.get(tableName)

      for (j <- Range(0, selectColumn.split(",").length)) {
        var column = selectColumn.split(",")(j)

        if (column.startsWith("#")) {
          column = column.replaceAll("#", "")
          sql.append(if (null == valueMap.get(column)) "null" else valueMap.get(column))
        } else {
          var o = r.getAs[String](column)
          if (o == null || "".equals(o.toString())) {
            sql.append("null")
          } else {
            var value = o.toString().replaceAll("'|/|\\\\", "");
            value = value.replaceAll("^\\s* |\\s*$", "");
            sql.append("'")
            sql.append(value)
            sql.append("'")
          }
        }
        if (j < selectColumn.split(",").length - 1)
          sql.append(",")

      }

      sql.append(")")

      rtnList.add(sql.toString())
    }
    rtnList
  }

  def batchInsert(sqlList: ArrayList[String], st: JdbcDBTemplate) = {
    try {
      val handler = new JdbcSqlHandler(sqlList.toArray(new Array[String](sqlList.size())))
      st.doIntrans(handler)
      LOG.info("++++++++++++sqlList size : " + sqlList.size())
      sqlList.clear();
    } catch {
      case t: Throwable => {
        LOG.error("batch insert exception ", t);
        //	      throw new RuntimeException("batch insert exception",t);
        var needTry = true
        if (t.isInstanceOf[UncategorizedSQLException]) {
          val ee = t.asInstanceOf[UncategorizedSQLException]
          if (ee.getSQLException().getErrorCode() == 1062) {
            LOG.error(t.getMessage, t);
            LOG.error("no need to retry");
            needTry = false;
          }
        }
        if (needTry) {
          val loop = new Breaks;
          loop.breakable {
            for (i <- Range(1, 21)) {
              LOG.info("jdbc batchUpdate retry no " + i);
              val handler = new JdbcSqlHandler(sqlList.toArray(new Array[String](sqlList.size())))
              st.doIntrans(handler)
              LOG.info("++++++++++++sqlList size : " + sqlList.size())
              sqlList.clear()
              loop.break
            }
          }

          LOG.error("Here is the failure of 20 times sql :");
        }
      }
    }
  }
}