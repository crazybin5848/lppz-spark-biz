package com.lppz.spark.hbase;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import com.lppz.spark.bean.SparkHiveSqlBean;
import com.lppz.spark.bean.SparkSqlConfigBean;
import com.lppz.spark.scala.HiveContextUtil;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.transfer.ParseNewSparkTransfer;
import com.lppz.spark.util.SparkHiveUtil;
import com.lppz.spark.util.SparkYamlUtils;

public class GenerateAllOrderSparkTable {
	private static Logger logger = Logger.getLogger(GenerateAllOrderSparkTable.class); 
	public static void main(String[] args) throws Exception {
		 args = new String[] { "/Users/zoubin/Documents/scalawork/lppz-spark-biz/lppz-spark-hbaseJedis/src/main/resources/META-INF/multimysql2hive.yaml", "local[8]",
		 "month,;2016-09,;maxdate,;'2016-09',;mindate,;'2016-07'" };
		if (args.length == 0)
			throw new IOException("need yaml config");
		GenerateAllOrderSparkTable ga=new GenerateAllOrderSparkTable();
		SparkHiveSqlBean bean = SparkYamlUtils.loadYaml(args[0], false);
		ParseNewSparkTransfer.parseAll(bean, args);
		MysqlSpark mysql = new MysqlSpark();
		String appName="batch create hive tb";
		SparkContext sc = mysql.buildSc(appName, args[1]);
		try{
//			HiveContextUtil.exec(sc, "drop schema omsext cascade");
//			HiveContextUtil.exec(sc, "create schema omsext");
			ga.generateAll(bean, args[1],sc);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			throw e;
		}
		finally{
			sc.stop();
		}
	}

	public void generateAll(SparkHiveSqlBean bean, String mode, SparkContext sc) throws Exception {
		try {
			doCreateHiveTable(bean, mode,sc);
			if (bean.getSparkMapbean() != null && bean.getSparkMapbean().size() != 0)
				circulateGenerate(bean.getSparkMapbean(), mode, bean.getConfigBean(),sc);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			throw e;
		}
	}

	private void circulateGenerate(Map<String, SparkHiveSqlBean> sparkMapbean, String mode,
			SparkSqlConfigBean configBean, SparkContext sc) throws Exception {
		for (Entry<String, SparkHiveSqlBean> entry : sparkMapbean.entrySet()) {
			try {
				if (entry.getValue().isMysqsqlUseMain())
					entry.getValue().setConfigBean(configBean);
				doCreateHiveTable(entry.getValue(), mode,sc);
				if (entry.getValue().getSparkMapbean() != null && entry.getValue().getSparkMapbean().size() != 0)
					circulateGenerate(entry.getValue().getSparkMapbean(), mode, entry.getValue().getConfigBean(),sc);
			} catch (Exception e) {
				logger.error(e.getMessage(),e);
				throw e;
			}
		}
	}

	private void doCreateHiveTable(SparkHiveSqlBean bean, String mode, SparkContext sc) throws SQLException {
		if (bean.getSourcebean().isMode()) {
			try {
				SparkHiveUtil.createHiveTableFromRDBMS(bean, mode,sc);
			} catch (SQLException e) {
				logger.error(e.getMessage(),e);
				throw e;
			}
		}
	}
}