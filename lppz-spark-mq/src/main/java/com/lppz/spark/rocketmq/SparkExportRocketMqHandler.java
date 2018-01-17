package com.lppz.spark.rocketmq;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.yaml.snakeyaml.Yaml;

import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.scala.rocketmq.SparkExportRocketMqBean;

public class SparkExportRocketMqHandler {
	private static final Logger LOG = Logger
			.getLogger(SparkExportRocketMqHandler.class);
	public static void main(String[] args) throws IOException {
		args=new String[]{"local","/home/licheng/workspace/Spark-History-Order/src/main/resources/META-INF/sparkexport.yaml",
				"192.168.37.243:9876;192.168.37.242:9876;192.168.37.247:9876",
				"/home/licheng/workspace/Spark-History-Order/src/main/resources/META-INF/jedis-cluster.yaml","2015-07"};
		SparkContext sc=null;
		String mode=args[0];
		try {
			if (args.length == 0)
				throw new IOException("need yaml config");
			String mainYamlPath=args[1];
			SparkExportRocketMqBean sebean=new Yaml().loadAs(new FileInputStream(mainYamlPath), SparkExportRocketMqBean.class);
			MysqlSpark mysql = new MysqlSpark();
			String appName = "export spark data to kafka";
			sc = mysql.buildSc(appName, mode);
			String kafkaBrokerPath = args[2];
			String jedisClusterPath = args[3];
			String month=args[4];
			sebean.loopExec(sc,kafkaBrokerPath, jedisClusterPath,month);
		}catch(Exception ex){
			LOG.error(ex.getMessage(),ex);
		}
		finally{
			if(sc!=null)
			sc.stop();
		}
		
	}
}
