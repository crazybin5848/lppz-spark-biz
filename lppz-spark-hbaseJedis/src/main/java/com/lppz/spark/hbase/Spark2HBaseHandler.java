package com.lppz.spark.hbase;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.yaml.snakeyaml.Yaml;

import com.lppz.spark.scala.AbstractHbaseTransCoder;
import com.lppz.spark.scala.bean.Spark2HbaseBean;
import com.lppz.spark.scala.jdbc.MysqlSpark;

public class Spark2HBaseHandler {
	private static final Logger LOG = Logger
			.getLogger(Spark2HBaseHandler.class);
	public static void main(String[] args) throws IOException {
//		args=new String[]{"local[8]","/Users/zoubin/Documents/scalawork/lppz-spark-biz/lppz-spark-hbaseJedis/src/main/resources/META-INF/spark2hbase_4.yaml",
//				"/Users/zoubin/Documents/scalawork/lppz-spark-biz/lppz-spark-hbaseJedis/src/main/resources/META-INF/jedis-cluster.yaml",
//				"/Users/zoubin/Documents/scalawork/lppz-spark-biz/lppz-spark-hbaseJedis/src/main/resources/META-INF/jedis-sentinel.yaml","2015-07","500","0","8","com.lppz.spark.scala.OmsHbaseTransCoder"};
//		args=new String[]{"local[32]","/Users/zoubin/Documents/scalawork/lppz-spark-biz/lppz-spark-hbaseJedis/src/main/resources/META-INF/spark2hbaseAll.yaml",
//				"/Users/zoubin/Documents/scalawork/lppz-spark-biz/lppz-spark-hbaseJedis/src/main/resources/META-INF/jedis-cluster.yaml",
//				"/Users/zoubin/Documents/scalawork/lppz-spark-biz/lppz-spark-hbaseJedis/src/main/resources/META-INF/jedis-sentinel.yaml","2015-07","50","50","5","com.lppz.spark.scala.OmsHbaseTransCoder"};
		SparkContext sc=null;
		String mode=args[0];
		try {
			if (args.length == 0)
				throw new IOException("need yaml config");
			String mainYamlPath=args[1];
			Spark2HbaseBean shbean=new Yaml().loadAs(new FileInputStream(mainYamlPath), Spark2HbaseBean.class);
			MysqlSpark mysql = new MysqlSpark();
			String appName = "load spark data to hbase";
			sc = mysql.buildSc(appName, mode);
			String jedisClusterPath=args[2];
			String jedisSentinalPath=args[3];
			String month=args[4];
			String batchNum=args[5];
			String pageNum=args[6];
			String partitionNum=args[7];
			String className=args[8];
			AbstractHbaseTransCoder coder=(AbstractHbaseTransCoder) Class.forName(className).newInstance();
			Map<String,String> paramMap=coder.buildParamMap();
			shbean.loopExec(sc,jedisClusterPath,jedisSentinalPath,month,Integer.parseInt(batchNum),Long.parseLong(pageNum),Integer.parseInt(partitionNum), paramMap,coder.getTransferCoder());
		}catch(Exception ex){
			LOG.error(ex.getMessage(),ex);
		}
		finally{
			if(sc!=null)
			sc.stop();
		}
	}
}