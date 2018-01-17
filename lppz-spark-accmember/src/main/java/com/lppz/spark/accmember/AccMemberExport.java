package com.lppz.spark.accmember;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import com.lppz.spark.accmember.bean.AccExportBean;
import com.lppz.spark.accmember.bean.TableBean;
import com.lppz.spark.accmember.scala.AccCalcMemberScoreHandler;
import com.lppz.spark.accmember.scala.AccExportHandler;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.util.SparkHiveUtil;
import com.lppz.spark.util.SparkYamlUtils;

public class AccMemberExport {
	
	private static Logger log = Logger.getLogger(AccMemberExport.class);
	
	static boolean isTest=false;
	static Long totalOnce=500000l;

	public static void main(String[] args) throws Exception{
//		args = new String[] { "F:\\workspace\\lppz-spark-biz\\lppz-spark-accmember\\src\\main\\resources\\table.yaml"
//		,"F:\\workspace\\lppz-spark-biz\\lppz-spark-mysql\\src\\main\\resources\\datasource.yaml"
//		,"F:\\workspace\\lppz-spark-biz\\lppz-spark-accmember\\src\\main\\resources\\jedis-cluster.yaml"
//		, "local[4]","genFile,;true,;max,;1000,;min,;1,;tableId,;3,;batch,;false"};

		if (args.length == 0)
			throw new IOException("need yaml config");
		
		String[] params = args[args.length - 1].split(",;");
		String tableId = null;
		boolean batch=false;
		boolean genFile=false;
		long max=0l;
		long min=0l;
		for(int i=0;i<params.length;){
			if("tableId".equals(params[i]))
				tableId = params[i+1];
			if("batch".equals(params[i]))
				batch = Boolean.valueOf(params[i+1]);
				isTest= batch;
			if("genFile".equals(params[i]))
				genFile = Boolean.valueOf(params[i+1]);
			if("max".equals(params[i]))
				max = Long.parseLong(params[i+1]);
			if("min".equals(params[i]))
				min = Long.parseLong(params[i+1]);
			i+=2;
		}
		String jedisClusterYamlPath=args[2];
		String dataSourcePath = args[1];
		SparkContext sc=null;
		AccExportBean bean =null;
		File yaml=new File(args[0]);
		try{
			bean = SparkYamlUtils.loadYaml(yaml.getAbsolutePath(), isTest,AccExportBean.class);
			if(bean != null){
				String appName = "export data  " + bean.getSourceUser() + ":" + SparkHiveUtil.now();
				totalOnce=bean.getTotalOnce()==null?totalOnce:bean.getTotalOnce();
				sc = new MysqlSpark().buildSc(appName, args[3]);
//				sc.conf().set("spark.sql.warehouse.dir", "/spark-warehouse/");
				TableBean table=bean.getTables().get(tableId);
				
				//前置作业
				if(tableId.equals("3")){
					execSpark2Jedis(sc,min,max,jedisClusterYamlPath,bean);
				}
				
				HashMap<String,String> enumTable=loadEnumTable(bean);
				
				AccExportHandler handler=new AccExportHandler();
				long k=0;
//				long max=10000000;//fetchMap.get("max");
//				long min=1;//fetchMap.get("min");
				for (long i = min; i <= max; i += totalOnce+1) {
					k=totalOnce + i>max?max:totalOnce + i;
					if(genFile){
						//导出文本文件
						handler.startExportGenFile(sc,bean,table,dataSourcePath,jedisClusterYamlPath, i, k,batch,enumTable);
					}else{
						//结果集导出直接写数据库
						handler.startExportWithView(sc,bean,table,dataSourcePath, i, k,batch,enumTable);
					}
				}
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
	
	private static void execSpark2Jedis(SparkContext sc, long min, long max,String jedisClusterYamlPath,AccExportBean bean) {
		Map<String,Object> params1=genJedisBean(bean);
		AccCalcMemberScoreHandler jedisHandler=new AccCalcMemberScoreHandler();
		
		long k=0;
		for (long i = min; i <= max; i += totalOnce+1) {
			k=totalOnce + i>max?max:totalOnce + i;
			jedisHandler.startExportToRedisCuster(sc, (AccExportBean)params1.get("bean"), (TableBean)params1.get("table"), jedisClusterYamlPath, i, k);
		}
	}

	private static Map<String,Object> genJedisBean(AccExportBean aeb){
		AccExportBean bean =new AccExportBean();

	    bean.setPartition(aeb.getPartition());
		bean.setSourceDriver(aeb.getSourceDriver());
		
		bean.setSourceJdbcUrl(aeb.getSourceJdbcUrl());
		bean.setSourcePwd(aeb.getSourcePwd());
		bean.setSourceUser(aeb.getSourceUser());
		
		TableBean table=new TableBean();
		table.setBatchSize(300);
		table.setPartition(200);
		table.setPrimaryKey("_id");
		table.setSourceTableOrView("lp_memberscores_view");
		
		Map<String,Object> rtnMap=new HashMap<>();
		rtnMap.put("bean", bean);
		rtnMap.put("table", table);
		return rtnMap;
	}
	
	private static HashMap<String,String> loadEnumTable(AccExportBean master){
		HashMap<String,String> rtnMap=new HashMap<>();
		
		Connection conn = null;
		String sql = "select PK,Code from acc.enumerationvalues";
		String url = master.getSourceJdbcUrl();
		try {
			Class.forName(master.getSourceDriver());
			conn = DriverManager.getConnection(url, master.getSourceUser(),
					master.getSourcePwd());
			PreparedStatement stmt = conn.prepareStatement(sql);
			
			ResultSet rs = stmt.executeQuery();
			
			while (rs.next()) {
				rtnMap.put(rs.getString(1), rs.getString(2));
			}
			return rtnMap;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			try {
				if(conn!=null){
					conn.close();
				}
			} catch (SQLException e) {
			}
		}
		
		return null;
	}
	
	
	private static Map<String,Long> fetchMaxAndMin(AccExportBean master,TableBean bean,String start,String end) throws Exception {
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

}
