package com.lppz.spark.hbase;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import com.lppz.spark.scala.HiveContextUtil;
import com.lppz.spark.scala.QueryScalaHandler;
import com.lppz.spark.scala.jdbc.MysqlSpark;

public class SparkJedisHandler {
	private static final Logger LOG = Logger
			.getLogger(SparkJedisHandler.class);
	public static void main(String[] args) throws IOException {
		SparkContext sc=null;
		String month=null;
		String mode=null;
		args=new String[]{"mode,;local,;month,;2015-07,;maxdate,;'2015-08-01',;mindate,;'2015-07-31',;dsPath,;/Users/chenlisong/Documents/lppz_code/lppz-spark-biz/lppz-spark-repeatQuery/src/main/resources/META-INF/jedis-cluster.yaml"};
		try {
			if (args.length == 0)
				throw new IOException("need yaml config");
			String[] params = args[0].split(",;");
			LOG.info("args[0]:"+args[0]);
			String dsPath=null;
			for(int i=0;i<params.length;){
				if("mode".equals(params[i]))
					mode=params[i+1];
				if("month".equals(params[i]))
					month = params[i+1];
				if("dsPath".equals(params[i]))
					dsPath = params[i+1];
				i+=2;
			}
			MysqlSpark mysql = new MysqlSpark();
			String appName = "Exec all query mysql sql";
			sc = mysql.buildSc(appName, mode);
			HiveContextUtil.exec(sc, "use tmp");
			String sqlStr="select field_1 from diffqj17_2 limit 1000";
			LOG.info("SparkDelsql2:"+sqlStr);
			QueryScalaHandler jd=new QueryScalaHandler();
			jd.exec(sc, sqlStr, dsPath);
		}catch(Exception ex){
			LOG.error(ex.getMessage(),ex);
		}
		finally{
			if(sc!=null)
			sc.stop();
		}
	}
}