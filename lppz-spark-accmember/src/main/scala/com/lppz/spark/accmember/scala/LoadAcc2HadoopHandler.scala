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
import com.lppz.spark.scala.SparkHdfsUtil
import org.apache.hadoop.fs.FileSystem
import com.lppz.spark.scala.jdbc.MysqlSpark
import org.apache.spark.HashPartitioner

class LoadAcc2HadoopHandler extends Serializable {
  @transient lazy val LOG = Logger.getLogger(getClass.getName)

  def load(sc: SparkContext,jdbdUrl:String,user:String,pwd:String,table:String,port:String,max:Long,min:Long){
    //初始化sql上下文
    val sqlContext = new SQLContext(sc)

    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", pwd)

    //初始化数据库操作对象dataframe
    var df = sqlContext.read.jdbc(jdbdUrl, table, "_id", min, max, 200, prop)
    //创建或更新视图
    df.createOrReplaceTempView(table)
//    val sql = "select m._id,l.partner_no,l.card_no,l.member_no,l.telphone,l.source_type,l.member_account,l.member_pwd,l.password_encoding,l.birthday,l.create_time,l.create_user,l.edit_time ,l.edit_user,l.register_store,l.recommender,l.member_status,l.sync_crm_status,l.sync_crm_time,l.member_type,l.member_level,l.zzannual_amt,l.zztier_count,l.email,l.register_time"
//          .concat(" from lp_member l right join lp_member_seq m on l.member_no=m.member_no where ")
//    		  .concat("m._id").concat(" between ").concat(String.valueOf(min)).concat(" and ").concat(String.valueOf(max))
    //组装执行sql
    val sql = "select * from ".concat(table.concat(" where ")
      .concat("_id").concat(" between ").concat(String.valueOf(min)).concat(" and ").concat(String.valueOf(max)))
      //设置到sql上下文中
      df = df.sqlContext.sql(sql)
    
    val typedata="NULL"
    //定义hdfs文件名和存放路径
    val fileName = "hdfs://hamaster:9000/tmp/acc2hive/" + table + "/" + port+"/"+min + "_" + max
    //遍历结果集
    df.rdd.map { r =>
      {
        val s: StringBuilder = new StringBuilder("")
        //遍历行中的列,每行数据生成一行文本
        for (j <- Range(0, r.length)) {
          var o = r.get(j)
          //特殊字符处理，null替换为NULL
          if (o == null)
            o = typedata;
          if (o.isInstanceOf[String]) {
            if ("null".equals(o.asInstanceOf[String].toLowerCase()))
              o = typedata
              o = (o.toString()).replaceAll("\r", "");
		          o = (o.toString()).replaceAll("\n", "");
		          o = (o.toString()).replaceAll("\t", " ");
		          o = (o.toString()).replaceAll("\\\\", "\\\\\\\\");
		          o = (o.toString()).replaceAll("\"", "");
          }
          s.append(o)
          if (j < r.length - 1)
            s.append("\t")
        }
        s
      }
      //结果写到hdfs上
    }.saveAsTextFile(fileName)
    
    var hdfs: FileSystem = null
    val mysql = new MysqlSpark()
    hdfs = new SparkHdfsUtil().getFileSystem("hdfs://hamaster:9000")
    val destFileName = "hdfs://hamaster:9000" + "/tmp/accGen2hive/" + table + "/" + port+"/" +min + "_" + max+ ".txt"
    //合并文本文件到指定目录
    mysql.mergeHdfsFile(hdfs, destFileName, fileName, "hdfs://hamaster:9000")
  }
  
}