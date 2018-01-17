package com.lppz.spark.accmember;

import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import com.lppz.spark.accmember.bean.AccExportBean;
import com.lppz.spark.accmember.bean.TableBean;
import com.lppz.spark.accmember.scala.AccCalcMemberScoreHandler;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.util.SparkHiveUtil;

public class AccScoreGroup {
	
	private static Logger log = Logger.getLogger(AccScoreGroup.class);
	
	static boolean isTest=false;
	static Long totalOnce=5000000l;

	public static void main(String[] args) throws Exception{
//		args = new String[] {"F:\\workspace\\lppz-spark-biz\\lppz-spark-accmember\\src\\main\\resources\\jedis-cluster.yaml"
//		,"local[4]"
//		,"genFile,;true,;max,;1000,;min,;1,;tableId,;3,;batch,;false"};

		if (args.length == 0)
			throw new IOException("need yaml config");
		
		String[] params = args[args.length - 1].split(",;");
		long max=0l;
		long min=0l;
		for(int i=0;i<params.length;){
			if("max".equals(params[i]))
				max = Long.parseLong(params[i+1]);
			if("min".equals(params[i]))
				min = Long.parseLong(params[i+1]);
			if("batch".equals(params[i]))
				isTest=Boolean.parseBoolean(params[i+1]);
			i+=2;
		}
		
		String jedisClusterYamlPath=args[0];
		SparkContext sc=null;
		AccExportBean bean =new AccExportBean();

	    bean.setPartition(100);
		bean.setSourceDriver("com.mysql.jdbc.Driver");
		
		if(isTest){
			bean.setSourceJdbcUrl("jdbc:mysql://192.168.19.74:3306/acc_70?useConfigs=maxPerformance&characterEncoding=utf8");

			bean.setSourcePwd("acc123");
			bean.setSourceUser("acc");
		}else{
			bean.setSourceJdbcUrl("jdbc:mysql://10.8.202.231:3311/acc_export?useConfigs=maxPerformance&characterEncoding=utf8");

			bean.setSourcePwd("lppzacc");
			bean.setSourceUser("root");
		}
		
		TableBean table=new TableBean();
		table.setBatchSize(300);
		table.setPartition(200);
		table.setPrimaryKey("_id");
		table.setSourceTableOrView("lp_memberscores_view");
		try{
			String appName = "export data  " + bean.getSourceUser() + ":" + SparkHiveUtil.now();
			totalOnce=bean.getTotalOnce()==null?totalOnce:bean.getTotalOnce();
			sc = new MysqlSpark().buildSc(appName, args[1]);
//			sc.conf().set("spark.sql.warehouse.dir", "/spark-warehouse/");
			
			AccCalcMemberScoreHandler handler=new AccCalcMemberScoreHandler();
			long k=0;
			for (long i = min; i <= max; i += totalOnce+1) {
				k=totalOnce + i>max?max:totalOnce + i;
				
				handler.startExportToRedisCuster(sc, bean, table, jedisClusterYamlPath, i, k);
			}
			log.info("export end;");
		}catch(Exception ex){
			log.error(ex.getMessage(), ex);
		}
		
		if(sc!=null){
			sc.stop();
		}
	}

}
