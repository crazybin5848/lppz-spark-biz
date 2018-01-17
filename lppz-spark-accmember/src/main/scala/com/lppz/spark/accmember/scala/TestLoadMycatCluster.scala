package com.lppz.spark.sap.scala;

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
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
import com.lppz.spark.scala.HiveContextUtil
import com.lppz.spark.scala.SparkHdfsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import com.lppz.spark.util.SparkYamlUtils
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.control.Breaks
import com.lppz.spark.scala.jdbc.SparkMycatClusterJdbcTemplete
import org.springframework.jdbc.UncategorizedSQLException
import com.lppz.spark.bean.jdbc.JdbcDBTemplate
import com.lppz.spark.scala.jdbc.JdbcSqlHandler
import java.lang.Boolean

object TestLoadMycatCluster extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName) 
  
  def main(args : Array[String]) = {
      val sparkConf = new SparkConf().setAppName("testAccMycat")
                      .setMaster(args(0))
      val sc = new MysqlSpark().buildSc("testAccMycat", args(0))
      HiveContextUtil.exec(sc, "use acc")
      try{
         val i=Integer.parseInt(args(4));
//       val loop = new Breaks;
//       loop.breakable {
//      while(true){
//         if(i>=5*Integer.parseInt(args(4)))
//           loop.break();
//         val sql = "select partner_no,card_no,member_no,telphone,source_type,member_account,member_pwd,password_encoding,birthday,create_time,create_user,edit_time,edit_user,register_store,recommender,member_status,sync_crm_status,sync_crm_time,member_type,member_level,zzannual_amt,zztier_count,email,register_time from lp_member where _id>="+i+" and _id<"+(i+Integer.parseInt(args(4)))
//         LOG.info("sqlFuck:"+sql)
//         HiveContextUtil.getRDD(sc, sql).repartition(500).foreachPartition { riter => 
//           handle(riter,args(1),Integer.parseInt(args(2)),java.lang.Boolean.parseBoolean(args(3)))
//         }
//         i+=Integer.parseInt(args(4))
//        }
//      }
         val sql = "select partner_no,card_no,member_no,telphone,source_type,member_account,member_pwd,password_encoding,birthday,create_time,create_user,edit_time,edit_user,register_store,recommender,member_status,sync_crm_status,sync_crm_time,member_type,member_level,zzannual_amt,zztier_count,email,register_time from lp_member where _id>"+i+" and _id<="+(Integer.parseInt(args(5)))
         LOG.info("sqlFuck:"+sql)
         HiveContextUtil.getRDD(sc, sql).repartition(500).foreachPartition { riter => 
           handle(riter,args(1),Integer.parseInt(args(2)),java.lang.Boolean.parseBoolean(args(3)))
         }
      }catch {
        case t: Throwable => LOG.error(t.getMessage,t)
      }
     finally{
       if(sc!=null)
         sc.stop()  
     }
  }
  
  def handle(it: Iterator[Row],dataSourcePath:String,batchSize:Integer,conCurrent:Boolean){
    val sqlList = new ArrayList[String]()
      object InternalSparkJdbcUtil extends SparkMycatClusterJdbcTemplete {}
      InternalSparkJdbcUtil.instance.buildJdbcTemplete(dataSourcePath)
      val st = InternalSparkJdbcUtil.instance.getSt()
      try{
         it.foreach { row =>
        val sql = buildInsertSql(row)
        sqlList.add(sql)
        if (!conCurrent && sqlList.size() >= batchSize) {
          batchInsert(sqlList, st);
        }
      }
      if (!conCurrent && !sqlList.isEmpty()) {
        LOG.info("not empty size " + sqlList.size())
        batchInsert(sqlList, st);
      }
      if (conCurrent && !sqlList.isEmpty()) {
        InternalSparkJdbcUtil.instance.mulitExec(sqlList, batchSize)
      }
      }
      finally{
         st.close()
      }
  }
  
  def buildInsertSql(r: Row):String={
    val s=new StringBuilder("Insert into lp_member(partner_no,card_no,member_no,telphone,source_type,member_account,member_pwd,password_encoding,birthday,create_time,create_user,edit_time,edit_user,register_store,recommender,member_status,sync_crm_status,sync_crm_time,member_type,member_level,zzannual_amt,zztier_count,email,register_time) values(")
    for (j <- Range(0, r.length)) {
          var o = r.get(j)
          if (o == null)
            o = "NULL";
          if (o.isInstanceOf[String]) {
            if ("null".equals(o.asInstanceOf[String].toLowerCase()))
              o = "NULL"
              o = o.toString().replaceAll("'", "");
              o = o.toString().replaceAll("\\\\", "");
          }
          if("NULL".equals(o)){
            s.append("null");
          }
          else{
            s.append("'")
            s.append(o)
            s.append("'")
          }
          if (j < r.length - 1)
            s.append(",")
        }
    s.append(")")
    s.toString()
  }
  
  def batchInsert(sqlList: ArrayList[String], st: JdbcDBTemplate) = {
    try {
      val handler = new JdbcSqlHandler(sqlList.toArray(new Array[String](sqlList.size())))
      st.doIntrans(handler)
//      LOG.info("++++++++++++sqlList size : " + sqlList.size())
    } catch {
      case t: Throwable => {
        LOG.error("batch insert exception ", t);
        //	      throw new RuntimeException("batch insert exception",t);
//        var needTry = true
        throw t;
//        if (t.isInstanceOf[UncategorizedSQLException]) {
//          val ee = t.asInstanceOf[UncategorizedSQLException]
//          if (ee.getSQLException().getErrorCode() == 1062) {
//            LOG.error(t.getMessage, t);
//            LOG.error("no need to retry");
//            needTry = false;
//          }
//        }
//        if (needTry) {
//          val loop = new Breaks;
//          loop.breakable {
//            for (i <- Range(1, 21)) {
//              LOG.info("jdbc batchUpdate retry no " + i);
//              val handler = new JdbcSqlHandler(sqlList.toArray(new Array[String](sqlList.size())))
//              st.doIntrans(handler)
//              LOG.info("++++++++++++sqlList size : " + sqlList.size())
//              sqlList.clear()
//              loop.break
//            }
//          }
//          LOG.error("Here is the failure of 20 times sql :");
//        }
      }
    }
    finally{
      sqlList.clear();
    }
  }
}