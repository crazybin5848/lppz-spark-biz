package com.lppz.spark.transfer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import org.apache.spark.SparkContext;

import com.lppz.spark.bean.SparkHiveSqlBean;
import com.lppz.spark.bean.SparkMysqlDmlBean;
import com.lppz.spark.bean.SparkSqlConfigBean;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.support.SparkContainer;
import com.lppz.spark.util.SparkHiveUtil;

public class GenerateSparkTransfer implements SparkTransfer{

	@Override
	public void excute(SparkHiveSqlBean bean, SparkSqlConfigBean config, String mode, SparkContainer sc,
			Map<String, Object> map, String month, String key, SparkMysqlDmlBean dmlBean, Map<String, Object> nextMap, String... args)
			throws IOException {
		if (bean.getSourcebean().isMode()) {
			if(sc.getSc()==null){
				MysqlSpark mysql = new MysqlSpark();
				String appName="batch create hive tb";
				SparkContext context = mysql.buildSc(appName, mode);
				sc.setSc(context);
				sc.setMysql(mysql);
			}
			try {
				SparkHiveUtil.createHiveTableFromRDBMS(bean, mode,sc.getSc());
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public Map<String, Integer> fetchMaxAndMin(SparkSqlConfigBean config, SparkMysqlDmlBean bean) {
		return null;
	}

}
