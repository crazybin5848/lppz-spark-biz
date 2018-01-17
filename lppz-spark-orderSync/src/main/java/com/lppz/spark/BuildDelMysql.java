package com.lppz.spark;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import com.lppz.spark.scala.HistoryOrderDelHandler;
import com.lppz.spark.scala.HiveContextUtil;
import com.lppz.spark.scala.jdbc.MysqlSpark;

public class BuildDelMysql {
	private static Logger logger = Logger.getLogger(BuildDelMysql.class); 
	public static void main(String[] args) throws IOException {
//		args = new String[] { "./META-INF/multimysql2hive.yaml", "local[128]", "month,;2015-08,;maxdate,;'2015-08-02',;mindate,;'2015-08-01',;span,;0,;total4Once,;50000" };
		SparkContext sc=null;
		String mode=args[0];
		String month=args[1];
		String fileDir=args[2];
		BuildDelMysql bb=new BuildDelMysql();
		try {
			if (args.length == 0)
				throw new IOException("need yaml config");
			MysqlSpark mysql = new MysqlSpark();
			String appName = "build all delete mysql sql";
			sc = mysql.buildSc(appName, mode);
			bb.build(fileDir,mysql, sc, mode, month);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		finally{
			if(sc!=null)
			sc.stop();
		}
	}

	private void build(String fileDir,MysqlSpark mysql, SparkContext sc, String mode, String month) throws IOException {
		HistoryOrderDelHandler hh=new HistoryOrderDelHandler();
		HiveContextUtil.exec(sc, "use omsextdel");
		String sqlStr="select a.schemaname as schema,a.tablename as tbl,a.colname as col,a.condpk as pk,a.wherecond as cond,b.colinstr as colin from omsdel a left join omsdeldetail b on a.condpk=b.id where substr(a.ds,1,7) ='"+month+"' and substr(b.ds,1,7) ='"+month+"'";
		logger.info("SparkDelsql2:"+sqlStr);
		hh.buildNewDelRdd(sc, sqlStr, fileDir+"/dele2");
		sqlStr="select schemaname as schema,tablename as tbl,colname as col,condpk as pk,wherecond as cond from omsdel where tablename='order_sharding' and substr(ds,1,7) ='"+month+"'";
		logger.info("SparkDelsql1:"+sqlStr);
		hh.buildNewDelRdd(sc, sqlStr, fileDir+"/dele1");
	}
}