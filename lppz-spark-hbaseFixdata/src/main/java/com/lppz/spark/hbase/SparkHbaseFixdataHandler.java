package com.lppz.spark.hbase;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.yaml.snakeyaml.Yaml;

import com.lppz.spark.bean.Parameter;
import com.lppz.spark.scala.FixDataScalaHandler;
import com.lppz.spark.scala.HiveContextUtil;
import com.lppz.spark.scala.bean.HiveBean;
import com.lppz.spark.scala.jdbc.MysqlSpark;

/**
 * scala 版本， 清理hbase重复数据
 * @author chenlisong
 *
 */
public class SparkHbaseFixdataHandler {
	private static final Logger LOG = Logger.getLogger(SparkHbaseFixdataHandler.class);
	public static void main(String[] args) throws IOException {
		long time = System.currentTimeMillis();
		
		SparkContext sc=null;

		if(args == null || args.length <= 0) {
			args=new String[]{"/Users/chenlisong/Documents/binlppzsap/lppz/lppz-spark-biz/lppz-spark-hbaseFixdata/src/main/resources/parameter.yaml"};
		}
		try {
			if (args.length == 0)
				throw new IOException("need yaml config");
			String parameterPath = args[0];
			Parameter parameter = new Yaml().loadAs(new FileInputStream(new File(parameterPath)), Parameter.class);
			
			
			MysqlSpark mysql = new MysqlSpark();
			String appName = "exec pull data from hive.";
			sc = mysql.buildSc(appName, parameter.getMode());
			
			HiveBean hiveBean = new Yaml().loadAs(new FileInputStream(new File(parameter.getDsPath())), HiveBean.class);
			
			HiveContextUtil.exec(sc, String.format("use %s", hiveBean.getHiveSchema()));
			String sqlStr = String.format("select orderid from %s where ds='%s'", hiveBean.getHiveTableName(), parameter.getDay());
			LOG.info("sql:"+sqlStr);
			
			FixDataScalaHandler handler = new FixDataScalaHandler();
			handler.exec(sc, sqlStr, parameter.getHbasePath(), parameter.getRowkeyPath());
			LOG.info(String.format("execute success, cost: %d s", (System.currentTimeMillis() - time)/1000));
			
		}catch(Exception ex){
			ex.printStackTrace();
			LOG.error(ex.getMessage(),ex);
		}
		finally{
			if(sc!=null)
			sc.stop();
		}
	}
}