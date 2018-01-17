package com.lppz.spark;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.springframework.core.io.FileSystemResource;
import org.yaml.snakeyaml.Yaml;

import com.lppz.core.datasource.DynamicDataSource;
import com.lppz.core.datasource.LppzBasicDataSource;
import com.lppz.spark.scala.HiveContextUtil;
import com.lppz.spark.scala.jdbc.MysqlSpark;

public class MultiThreadExecDelSql {
	
	private static final Logger LOG = Logger
			.getLogger(MultiThreadExecDelSql.class);
	
	private static ExecutorService tp = Executors.newCachedThreadPool();
	
	private static DynamicDataSource dataSource;
	
	private static int pageSize=0;

	public static void main(String[] args) {
		args=new String[]{"local[8]","2015-07","10000"};
		String mode=args[0];
		String ds=args[1];
		pageSize=Integer.parseInt(args[2]);
		SparkContext sc=null;
		
		MysqlSpark mysql = new MysqlSpark();
		
		String appName = "Exec all delete mysql sql";
		sc = mysql.buildSc(appName, mode);
		
		MultiThreadExecDelSql multiThreadExecDelSql=new MultiThreadExecDelSql();
		multiThreadExecDelSql.initDs(null);
		
		HiveContextUtil.exec(sc, "use omsextdel");
		
		Row[] rows=HiveContextUtil.execQuery(sc, "select a.schemaname as schema,a.tablename as tbl,a.colname as col,a.condpk as pk,a.wherecond as cond,b.colinstr as colin from omsdel a left join omsdeldetail b on a.condpk=b.id where "+"' a.ds='"+ds+"' and b.ds='"+ds+"'");
		
		CountDownLatch cdl=new CountDownLatch(rows.length);
		
		if(null!=rows && rows.length>0){
			for(Row row:rows){
				tp.execute(new ExecThread(cdl,row));
			}
			try{
				cdl.await();
			}catch(Exception ex){}
			
			tp.shutdown();
		}
		LOG.info("=======MultiThreadExecDelSql Successfully=======");
	}
	
	static class ExecThread implements Runnable{
		
		private CountDownLatch cdl;
		
		private Row row;
		
		public ExecThread(){}
		
		public ExecThread(CountDownLatch cdl,Row row){
			this.cdl=cdl;
			this.row=row;
		}

		@Override
		public void run() {
			try{
				exec(row);
			}catch(Exception ex){
				LOG.error(ex.getMessage(),ex);
			}finally{
				this.cdl.countDown();
			}
		}
		
	}
	
	public static void exec(Row row)throws Exception{
		String wherecond=row.getString(5);
		String schema=row.getString(1);
		if(!wherecond.equals("NULL")){
			String tableName=row.getString(2);
			String colname=row.getString(3);
			StringBuilder sb=new StringBuilder();
			sb.append("delete from ").append(schema).append(".").append(tableName).append(" where ")
			.append(colname).append(" ").append(wherecond);
			
			execSql(sb.toString(),schema);
			
			return;
		}
		
		String colin=row.getString(6);
		
		String[] colins=colin.split(",");
		
		
		
		int page=colins.length % pageSize==0?colins.length/pageSize:(colins.length/pageSize+1);
		
		
		for(int i=0;i<=page;i++){
			String delsql=buildDelSql(row,i);
			
			execSql(delsql,schema);
		}
	}
	
	public static void execSql(String sql,String scheam)throws Exception{
		LppzBasicDataSource baseDataSource=(LppzBasicDataSource)dataSource.getTargetDataSources().get("ds-"+scheam);
		
		Connection conn=baseDataSource.getConnection();
		
		PreparedStatement ps=conn.prepareStatement(sql);
		ps.executeUpdate();
		
		if(null!=ps)
			ps.close();
		
		if(null!=conn)
			conn.close();
	}
	
	public static String buildDelSql(Row row,int page){
		String schema=row.getString(1);
		String tableName=row.getString(2);
		String colname=row.getString(3);
		String colin=row.getString(6);
		
		StringBuilder sb=new StringBuilder();
		
		String[] colins=colin.split(",");
		
		List<String> data=Arrays.asList(colins);
		
		
		
		sb.append("delete from ").append(schema).append(".").append(tableName)
		.append(" where ").append(colname).append(" in(");
		
		List<String> subList=data.subList(pageSize*(page), pageSize*(page+1)>data.size()?data.size():pageSize*(page+1));
		
		for(String str:subList){
			sb.append(str);
		}
		sb.append(")");
		
		LOG.info(sb);
		
		return sb.toString();
	}

	@SuppressWarnings("unchecked")
	public void initDs(String dsYamlConfPath) {
		Map<Object, Object> targetDataSources=null;
		try {
			if (StringUtils.isNotBlank(dsYamlConfPath)) {
				if (new File(dsYamlConfPath).exists()) {
					targetDataSources = (Map<Object, Object>) new Yaml()
					.load(new FileSystemResource(dsYamlConfPath)
					.getInputStream());
				} 
			}
			else {
				InputStream in=getClass().getResourceAsStream("/META-INF/ds-cluster.yaml");
				if(in!=null&&in.available()>0){
					targetDataSources = (Map<Object, Object>) new Yaml().load(in);
					in.close();
				}
			}
		} catch (IOException e) {
			LOG.error(e.getMessage(),e);
		}
		LppzBasicDataSource lb = (LppzBasicDataSource) targetDataSources
				.values().iterator().next();
		dataSource = new DynamicDataSource(lb, targetDataSources);
	}

	public static DynamicDataSource getDataSource() {
		return dataSource;
	}

	public static void setDataSource(DynamicDataSource dataSource) {
		MultiThreadExecDelSql.dataSource = dataSource;
	}

}
