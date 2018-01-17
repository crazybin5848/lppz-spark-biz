package com.lppz.spark.mysql;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import com.lppz.spark.mysql.bean.Hive2DbmsBean;
import com.lppz.spark.mysql.scala.InitAccMemberHandler;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.util.SparkHiveUtil;
import com.lppz.spark.util.SparkYamlUtils;

public class InitAccMemberSpark {
	
	private static Logger log = Logger.getLogger(InitAccMemberSpark.class);
	
	static boolean isTest=false;
	static long totalOnce = 6000000;

	public static void main(String[] args) throws Exception{
//		args = new String[] { "C:\\Users\\romeo\\Desktop\\lpmember.yaml", "local[8]","month,;'2015-07',;maxdate,;'2015-08-01',;mindate,;'2015-07-01'"};
		
		if (args.length == 0)
			throw new IOException("need yaml config");
		
		String[] params = args[args.length - 1].split(",;");
		String month = null;
		String maxdate = null;
		String mindate = null;
		for(int i=0;i<params.length;){
			if("month".equals(params[i]))
				month = params[i+1];
			if("maxdate".equals(params[i]))
				maxdate = params[i+1];
			if("mindate".equals(params[i]))
				mindate = params[i+1];
			i+=2;
		}
				
		SparkContext sc=null;
		Hive2DbmsBean bean =null;
		File yaml=new File(args[0]);
		bean = SparkYamlUtils.loadYaml(yaml.getAbsolutePath(), isTest,Hive2DbmsBean.class);
		
		String appName = "export hive table " + bean.getHiveSchema() + ":" + bean.getHiveTableName() + SparkHiveUtil.now();
		
		Map<String,Long> fetchMap= fetchMaxAndMin(bean,maxdate,mindate);

		if(null==fetchMap || fetchMap.isEmpty())
			throw new RuntimeException("hana table is empty");

		long max=fetchMap.get("max");
		long min=fetchMap.get("min");
		sc = new MysqlSpark().buildSc(appName, args[1]);
		InitAccMemberHandler handler=new InitAccMemberHandler();
		long k=0;
		for (long i = min; i <= max; i += totalOnce+1) {
			k=totalOnce + i>max?max:totalOnce + i;
			handler.initAccMember(appName, bean, sc, i, k, month);
		}
		
		log.info("export end;");
	}
	
	private static Map<String,Long> fetchMaxAndMin(Hive2DbmsBean bean,String max,String min) throws Exception {
		Connection conn = null;
//		String sql = "select max("+bean.getPrimaryKey()+"),min("+bean.getPrimaryKey()+") from " +bean.getRdbmsTableName()
//				+" where createdTS>="+min+" and createdTS<"+max;
		
		String sql = "select max("+bean.getPrimaryKey()+"),min("+bean.getPrimaryKey()+") from " +bean.getRdbmsTableName();
		String url = bean.getRdbmsJdbcUrl();
		try {
			Class.forName(bean.getRdbmsdbDriver());
			conn = DriverManager.getConnection(url, bean.getRdbmsJdbcUser(),
					bean.getRdbmsJdbcPasswd());
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			
			Map<String,Long> fetchMap=new HashMap<>();
			while (rs.next()) {
				fetchMap.put("max", rs.getLong(1));
				fetchMap.put("min", rs.getLong(2));
			}
			return fetchMap;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
			}
		}
	}

}
