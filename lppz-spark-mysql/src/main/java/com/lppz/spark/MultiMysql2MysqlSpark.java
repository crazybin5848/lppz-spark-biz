package com.lppz.spark;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import com.lppz.spark.mysql.bean.Db2DbBean;
import com.lppz.spark.mysql.bean.Db2DbTableBean;
import com.lppz.spark.mysql.scala.Db2DbHandler;
import com.lppz.spark.mysql.scala.Db2DbWithKafkaHandler;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.util.SparkHiveUtil;
import com.lppz.spark.util.SparkYamlUtils;

public class MultiMysql2MysqlSpark {
	
	private static Logger log = Logger.getLogger(MultiMysql2MysqlSpark.class);
	
	static boolean isTest=false;
	static boolean dataType=true;
	static boolean optimize=true;
	static Long totalOnce=500000l;

	public static void main(String[] args) throws Exception{
//		args = new String[] { "F:\\workspace\\lppz-spark-biz\\lppz-spark-mysql\\src\\main\\resources\\tableBeanTest.yaml"
//				,"F:\\workspace\\lppz-spark-biz\\lppz-spark-mysql\\src\\main\\resources\\datasource.yaml", "local[4]","maxdate,;2015-08-01,;mindate,;2015-07-01,;tableId,;1,;batch,;false"};
		
		if (args.length == 0)
			throw new IOException("need yaml config");
		
		String[] params = args[args.length - 1].split(",;");
		String maxdate = null;
		String mindate = null;
		String tableId = null;
		boolean batch=false;
		for(int i=0;i<params.length;){
			if("maxdate".equals(params[i]))
				maxdate = params[i+1];
			if("mindate".equals(params[i]))
				mindate = params[i+1];
			if("tableId".equals(params[i]))
				tableId = params[i+1];
			if("batch".equals(params[i]))
				batch = Boolean.valueOf(params[i+1]);
			i+=2;
		}
		
		String dataSourcePath = args[1];
		SparkContext sc=null;
		Db2DbBean bean =null;
		File yaml=new File(args[0]);
		try{
			bean = SparkYamlUtils.loadYaml(yaml.getAbsolutePath(), isTest,Db2DbBean.class);
			if(bean != null){
				String appName = "export data  " + bean.getSourceUser() + ":" + SparkHiveUtil.now();
				totalOnce=bean.getTotalOnce()==null?totalOnce:bean.getTotalOnce();
				sc = new MysqlSpark().buildSc(appName, args[2]);
				
//					List<Db2DbTableBean> tables =bean.getTables();
				
//					if(null!=tables && tables.size()>0){
//						for(Db2DbTableBean table:tables){
//							Map<String,Long> fetchMap=fetchMaxAndMin(bean, table, mindate, maxdate);
//									
//							Db2DbHandler handler=new Db2DbHandler();
//							
////							Db2DbWithKafkaHandler handler=new Db2DbWithKafkaHandler();
//							
//							long k=0;
//							long max=fetchMap.get("max");
//							long min=fetchMap.get("min");
//							for (long i = min; i <= max; i += totalOnce+1) {
//								k=totalOnce + i>max?max:totalOnce + i;
//								handler.startExportWithView(sc,bean,table,dataSourcePath, i, k);
////								handler.startExportWithView(sc,bean,table,dataSourcePath,bean.getKafkaBrokerPath(), i, k);
//							}
//						}
//					}
				Db2DbTableBean table=bean.getTables().get(tableId);
				Map<String,Long> fetchMap=fetchMaxAndMin(bean, table, mindate, maxdate);
				Db2DbHandler handler=new Db2DbHandler();
				long k=0;
				long max=fetchMap.get("max");
				long min=fetchMap.get("min");
				for (long i = min; i <= max; i += totalOnce+1) {
					k=totalOnce + i>max?max:totalOnce + i;
					handler.startExportWithView(sc,bean,table,dataSourcePath, i, k,batch);
				}
				
				
//					handler.startExport(sc, bean, dataSourcePath);
				log.info("export end;");
			}else{
				log.warn("Db2DbBean is null");
			}
		}catch(Exception ex){
			log.error(ex.getMessage(), ex);
		}
		
		if(sc!=null){
			sc.stop();
		}
	}
	
	public static List<String> calcDay(String start,String end) throws ParseException{
		java.text.SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
		
		int cnt=getIntervalDays(df.parse(start),df.parse(end));
		
		if(cnt>0){
			List<String> list=new ArrayList<>(cnt);
			
			Calendar c=Calendar.getInstance();
			c.setTime(df.parse(start));
			for(int i=0;i<cnt;i++){
				list.add(df.format(c.getTime()));
				
				c.add(Calendar.DATE, 1);
			}
			return list;
		}else{
			List<String> list=new ArrayList<>();
			list.add(start);
			return list;
		}
		
	}
	
	private static Map<String,Long> fetchMaxAndMin(Db2DbBean master,Db2DbTableBean bean,String start,String end) throws Exception {
		Connection conn = null;
//		String sql = "select max("+bean.getPrimaryKey()+"),min("+bean.getPrimaryKey()+") from " +bean.getRdbmsTableName()
//				+" where createdTS>="+min+" and createdTS<"+max;
		
		String sql = "select max(id),min(id) from " +bean.getSeqTable()+" where create_date>=? and create_date<?";
		String url = master.getSourceJdbcUrl();
		try {
			Class.forName(master.getSourceDriver());
			conn = DriverManager.getConnection(url, master.getSourceUser(),
					master.getSourcePwd());
			PreparedStatement stmt = conn.prepareStatement(sql);
			
			stmt.setString(1, start);
			stmt.setString(2, end);
			
			ResultSet rs = stmt.executeQuery();
			
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
				if(conn!=null){
					conn.close();
				}
			} catch (SQLException e) {
			}
		}
	}
	
	public static int getIntervalDays(java.util.Date startDate, java.util.Date endDate) {
		long startdate = startDate.getTime();
		long enddate = endDate.getTime();
		long interval = enddate - startdate;
		int intervaldays = (int) (interval / (1000 * 60 * 60 * 24));
		return intervaldays;
	}
	
	private static boolean checkDirectoryExist(String path){
		File file=new File(path);
		
		if(!file.exists())
			return false;
		
		if(file.isDirectory())
			return true;
		
		return false;
	}

}
