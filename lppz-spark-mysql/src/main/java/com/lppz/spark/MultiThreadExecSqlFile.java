package com.lppz.spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.core.io.FileSystemResource;
import org.yaml.snakeyaml.Yaml;

import com.lppz.core.datasource.DynamicDataSource;
import com.lppz.core.datasource.LppzBasicDataSource;

public class MultiThreadExecSqlFile {
	private static final Logger LOG = Logger
			.getLogger(MultiThreadExecSqlFile.class);
	
	private static ExecutorService tp = Executors.newCachedThreadPool();
	
	private static DynamicDataSource dataSource;

	public static void main(String[] args) {
//		args=new String[]{"C:\\Users\\romeo\\Desktop\\delsql;C:\\Users\\romeo\\Desktop\\ds-cluster_prod.yaml"};
		
		String[] params=args[0].split(";");
		
		String path=params[0];
//		String month=params[1];
		
		MultiThreadExecSqlFile multiThreadExecSqlFile=new MultiThreadExecSqlFile();
		
		multiThreadExecSqlFile.initDs(params[1]);
		
		File root=new File(path);
		
		File[] schemas=root.listFiles();
		
		AtomicInteger cnt=new AtomicInteger();
		
		int fileCount=0;
		long time=System.currentTimeMillis();
		for(File subFile:schemas){
			final String scheam=subFile.getName();
			
			fileCount+=subFile.listFiles().length;
			
			for(final File tableFile:subFile.listFiles()){
				try {
					tp.execute(new ExecThread(cnt,tableFile,scheam));
				} catch (Exception e) {
					LOG.error(e.getMessage(),e);
				}
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
			MultiThreadExecSqlFile.getDataSource().destory();
		} catch (IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
	static class ExecThread implements Runnable{
		
		private AtomicInteger cnt;
		
		private String scheam;
		
		private File file;
		
		public ExecThread(){}
		
		public ExecThread(AtomicInteger cnt,File file,String scheam){
			this.cnt=cnt;
			this.file=file;
			this.scheam=scheam;
		}

		@Override
		public void run() {
			try{
				exec(file,scheam);
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
	/*
	 * 	├─execFailSql
		│  ├─omsedb
		│  │  ├─busi_lack_order
		│  │  ├─busi_omsinterfacemutual_wms_data
		│  │  ├─catch_order_err_msg
		│  │  ├─temp_order
		│  │  ├─temp_order_line
		│  │  ├─temp_order_line_promotion_info
		│  │  ├─temp_payment_info
		│  │  ├─temp_return_order
		│  │  └─temp_return_order_line
		│  └─omsext
		│     ├─busi_lp_deliverye_data
		│     ├─busi_lp_deliverye_line_data
		│     ├─busi_lp_deliverye_line_data_b2b
		│     ├─busi_merge_order_pool_data
		│     ├─busi_omsinterfacemutual_data
		│     ├─busi_orderline_promotion_info
		│     ├─busi_promotion_info
		│     ├─busi_refund_only_data
		│     ├─busi_return_package_data
		│     ├─orderdata_srlocationids
		│     ├─orderlineattributes
		│     ├─orderlinedata_locationroles
		│     ├─orderlinequantities
		│     ├─orderlines
		│     ├─orders
		│     ├─paymentinfo
		│     ├─returnorderlines
		│     ├─returns
		│     └─shipments
	 */
	public static void exec(File tableFile,String scheam) throws Exception{
		if(null!=tableFile && tableFile.exists() && tableFile.isDirectory()){
			for(File sqlFile:tableFile.listFiles(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".sql");
				}
			})){
				BufferedReader reader=new BufferedReader(new FileReader(sqlFile));
				
				if(sqlFile.getName().toLowerCase().indexOf("order_sharding")>-1){
					reader.close();
					continue;
				}
				
				try {
					Connection conn=null;
					String tempSql=null;
					int line=1;
					while((tempSql=reader.readLine())!=null){
						PreparedStatement ps=null;
						LppzBasicDataSource baseDataSource=null;
						try {
							baseDataSource=(LppzBasicDataSource)dataSource.getTargetDataSources().get("ds-"+scheam);
							conn = baseDataSource.getConnection();
							ps = conn.prepareStatement(tempSql);
							int effectCount=ps.executeUpdate();
//							int effectCount=100;
							LOG.info("执行成功,删除 "+effectCount+" 行, "+scheam+"/"+sqlFile.getParentFile().getName()+"/"+sqlFile.getName()+" =>第"+line+"行");
							line++;
						} catch (Exception e) {
//							LOG.error("ERRORSQL:"+tempSql);
							LOG.error("执行失败:"+scheam+"/"+sqlFile.getParentFile().getName()+"/"+sqlFile.getName()+"=>第"+line+"行");
							LOG.error(e.getMessage(),e);
						}
						finally{
							if(null!=ps)
								ps.close();
							if(conn!=null)
								conn.close();
						}
					}
				} finally{
					reader.close();
				}
			}
		}
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
		MultiThreadExecSqlFile.dataSource = dataSource;
	}
}
