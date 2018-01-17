package com.lppz.spark.hbase;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import com.lppz.spark.scala.HiveContextUtil;
import com.lppz.spark.scala.jdbc.MysqlSpark;

public class LoadlocalOrderSparkTable {
	private static Logger logger = Logger.getLogger(LoadlocalOrderSparkTable.class); 
	public static void main(String[] args) throws Exception {
		MysqlSpark mysql = new MysqlSpark();
		String appName="batch load hive tb";
		SparkContext sc = mysql.buildSc(appName, "local[8]");
		try{
			HiveContextUtil.exec(sc, "use omsext");
			for(String tableName:new File("/opt/tmpfuck/").list()){
				String loadsql="load data local inpath '/opt/tmpfuck/"+tableName+"/ds=2015-07' overwrite into table "+tableName+" PARTITION (ds='2015-07')";
				System.out.println(loadsql);
				HiveContextUtil.exec(sc,loadsql);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			throw e;
		}
		finally{
			sc.stop();
		}
	}
}