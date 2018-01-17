package com.lppz.spark.oms.inittestdata;

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
			for(String tableName:new File("/home/licheng/data/spark-warehouse-oms/omsext.db").list()){
				String loadsql="load data local inpath '/home/licheng/data/spark-warehouse-oms/omsext.db/"+tableName+"/ds=2016-08' overwrite into table "+tableName+" PARTITION (ds='2016-08')";
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