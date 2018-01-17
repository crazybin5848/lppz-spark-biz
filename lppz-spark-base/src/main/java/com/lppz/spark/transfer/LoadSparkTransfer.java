package com.lppz.spark.transfer;

import java.io.IOException;
import java.util.Map;

import org.apache.spark.SparkContext;

import com.lppz.spark.bean.SparkHiveSqlBean;
import com.lppz.spark.bean.SparkMysqlDmlBean;
import com.lppz.spark.bean.SparkSqlConfigBean;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.support.SparkContainer;
import com.lppz.spark.util.SparkHiveUtil;

public class LoadSparkTransfer implements SparkTransfer{

	@Override
	public void excute(SparkHiveSqlBean bean, SparkSqlConfigBean config, String mode, SparkContainer sc,
			Map<String, Object> map, String month, String key, SparkMysqlDmlBean dmlBean, Map<String, Object> nextMap, String... args)
			throws IOException {
		if(sc.getSc()==null){
			MysqlSpark mysql = new MysqlSpark();
			String appName="batch load hive tb";
			SparkContext context = mysql.buildSc(appName, mode);
			sc.setSc(context);
			sc.setMysql(mysql);
		}
		SparkHiveUtil.doLoad2Hive(bean.getSourcebean(), mode, buildDir(bean, month,bean.getMysqlBean().getTableName()),sc.getSc(),false);
		
	}

	private String buildDir(SparkHiveSqlBean bean, String month, String tableName) {
		String fileDir = "/tmp/mysqldata/" + bean.getConfigBean().getSchema() + "/" + tableName+ "/" + month;
		return fileDir;
	}

	@Override
	public Map<String, Integer> fetchMaxAndMin(SparkSqlConfigBean config, SparkMysqlDmlBean bean) {
		return null;
	}

}
