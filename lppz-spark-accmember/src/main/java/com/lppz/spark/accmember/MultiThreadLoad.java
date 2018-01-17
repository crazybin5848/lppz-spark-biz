package com.lppz.spark.accmember;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.core.io.FileSystemResource;
import org.yaml.snakeyaml.Yaml;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lppz.core.datasource.DynamicDataSource;
import com.lppz.core.datasource.LppzBasicDataSource;
import com.lppz.spark.accmember.bean.LoadSqlBean;
import com.lppz.spark.util.SparkYamlUtils;

public class MultiThreadLoad implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7689725435400059919L;

	private static final Logger LOG = Logger
			.getLogger(MultiThreadLoad.class);

	public static void main(String[] args) {
		args=new String[]{"F:\\workspace\\lppz-spark-biz\\lppz-spark-accmember\\src\\main\\resources\\load.yaml",
				"F:\\workspace\\lppz-spark-biz\\lppz-spark-accmember\\src\\main\\resources"};

		String yaml=args[0];
		String path=args[1];
		
		File root=new File(path);
		
		if(!root.exists()){
			LOG.error("file not found");
			System.exit(-1);
		}
		LoadSqlBean bean=SparkYamlUtils.loadYaml(yaml,false,LoadSqlBean.class);
		
		
		ExecutorService tp = Executors.newFixedThreadPool(bean.getMyCatInfo().size());
		AtomicInteger cnt=new AtomicInteger();
		
		int fileCount=root.listFiles().length;
		long time=System.currentTimeMillis();
		
		for(final File tableFile:root.listFiles()){
			try {
				tp.execute(new ExecThread(cnt,tableFile,bean));
			} catch (Exception e) {
				LOG.error(e.getMessage(),e);
			}
		}
		
		while(cnt.get()!=fileCount){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		LOG.info("task over:exaust time:"+(System.currentTimeMillis()-time)/1000+"s");
		try {
			tp.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static class ExecThread implements Runnable{
		
		private AtomicInteger cnt;
		
		private File file;
		
		private LoadSqlBean bean;
		
		public ExecThread(){}
		
		public ExecThread(AtomicInteger cnt,File file,LoadSqlBean bean){
			this.cnt=cnt;
			this.file=file;
			this.bean=bean;
		}

		@Override
		public void run() {
			try{
				exec(file,bean);
			}catch(Exception ex){
				LOG.error(ex.getMessage(),ex);
			}finally{
				cnt.getAndIncrement();
			}
		}
		
	}
	
	public static File findFileDir(File file,String name){
		File[] files=file.listFiles();
		
		for(File f:files){
			if(f.getName().equals(name))
				return f;
		}
		
		return null;
	}
	
	public static void exec(File sqlFile,LoadSqlBean bean) throws Exception{
		if(sqlFile.exists()){
			Connection conn=null;
			PreparedStatement ps=null;
			String loadSql="";
			try {
				Class.forName(bean.getJdbcDriver());
//				conn = DriverManager.getConnection(url, bean.get,master.getSourcePwd());
				ps = conn.prepareStatement(loadSql);
				
//				loadSql="load data LOCAL infile '"+execFile.getAbsolutePath()+"' into table "+params[1]+" fields terminated by '\\t' lines terminated by '\\n'";
				
				int effectCount=ps.executeUpdate(loadSql);
				LOG.info("执行成功, "+loadSql+",影响行数: "+effectCount);
			} catch (Exception e) {
				LOG.error("执行失败:"+loadSql);
				LOG.error(e.getMessage(),e);
			}
			finally{
				if(null!=ps)
					ps.close();
				if(conn!=null)
					conn.close();
			}
		}
	}
}
