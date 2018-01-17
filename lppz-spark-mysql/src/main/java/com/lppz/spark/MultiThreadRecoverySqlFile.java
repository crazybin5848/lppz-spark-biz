package com.lppz.spark;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
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

public class MultiThreadRecoverySqlFile {
	private static final Logger LOG = Logger
			.getLogger(MultiThreadRecoverySqlFile.class);
	
	private static ExecutorService tp = Executors.newCachedThreadPool();
	
	private static DynamicDataSource dataSource;
	
	private static Map<String,String> fuckMap=new HashMap<String,String>();
	
	static{
		fuckMap.put("omsedbbusiLackOrder","omsedb,busi_lack_order");
		fuckMap.put("omsedbbusiOmsinterfacemutualWmsData","omsedb,busi_omsinterfacemutual_wms_data");
		fuckMap.put("omsedbcatchOrderErrMsg","omsedb,catch_order_err_msg");
		fuckMap.put("omsedbtempOrder","omsedb,temp_order");
		fuckMap.put("omsedbtempOrderLine","omsedb,temp_order_line");
		fuckMap.put("omsedbtempOrderLinePromotionInfo","omsedb,temp_order_line_promotion_info");
		fuckMap.put("omsedbtempPaymentInfo","omsedb,temp_payment_info");
		fuckMap.put("omsedbtempReturnOrder","omsedb,temp_return_order");
		fuckMap.put("omsedbtempReturnOrderLine","omsedb,temp_return_order_line");
		fuckMap.put("omsextbusiLpDeliveryeData","omsext,busi_lp_deliverye_data");
		fuckMap.put("omsextbusiLpDeliveryeLineData","omsext,busi_lp_deliverye_line_data");
		fuckMap.put("omsextbusiLpDeliveryeLineDataB2b","omsext,busi_lp_deliverye_line_data_b2b");
		fuckMap.put("omsextbusiMergeOrderPoolData","omsext,busi_merge_order_pool_data");
		fuckMap.put("omsextbusiOmsinterfacemutualData","omsext,busi_omsinterfacemutual_data");
		fuckMap.put("omsextbusiOrderlinePromotionInfo","omsext,busi_orderline_promotion_info");
		fuckMap.put("omsextbusiPromotionInfo","omsext,busi_promotion_info");
		fuckMap.put("omsextbusiRefundOnlyData","omsext,busi_refund_only_data");
		fuckMap.put("omsextbusiReturnPackageData","omsext,busi_return_package_data");
		fuckMap.put("omsextbusiReturnPickOrder","omsext,busi_return_pick_order");
		fuckMap.put("omsextbusiReturnPickOrderline","omsext,busi_return_pick_orderline");
		fuckMap.put("omsextorderdataSrlocationids","omsext,orderdata_srlocationids");
		fuckMap.put("omsextorderlineattributes","omsext,orderlineattributes");
		fuckMap.put("omsextorderlinedataLocationroles","omsext,orderlinedata_locationroles");
		fuckMap.put("omsextorderlinequantities","omsext,orderlinequantities");
		fuckMap.put("omsextorderlines","omsext,orderlines");
		fuckMap.put("omsextorders","omsext,orders");
		fuckMap.put("omsextorderSharding","omsext,order_sharding");
		fuckMap.put("omsextpaymentinfo","omsext,paymentinfo");
		fuckMap.put("omsextreturnorderlines","omsext,returnorderlines");
		fuckMap.put("omsextreturns","omsext,returns");
		fuckMap.put("omsextshipments","omsext,shipments");
		
	}

	public static void main(String[] args) {
//		args=new String[]{"F:\\workspace\\Spark-History-Order\\src\\main\\resources\\mysqlGen","F:\\workspace\\Spark-History-Order\\src\main\\resources\\META-INF\\dscluster.yaml"};
//		args=new String[2];
//		args[0]="F:\\workspace\\Spark-History-Order\\src\\main\\resources\\mysqlGen\\omsext;F:\\workspace\\Spark-History-Order\\src\\main\\resources\\META-INF\\ds-cluster.yaml;2015-07";
		
		String[] params=args[0].split(";");
		
		String path=params[0];
		String ds=params[1];
		String month=params[2];
		
		MultiThreadRecoverySqlFile multiThreadExecSqlFile=new MultiThreadRecoverySqlFile();
		
		multiThreadExecSqlFile.initDs(ds);
		
		File root=new File(path);
		
		if(!root.exists()){
			LOG.error("file not found");
			System.exit(-1);
		}
		
		AtomicInteger cnt=new AtomicInteger();
		
		int fileCount=root.listFiles().length;
		long time=System.currentTimeMillis();
		
		for(final File tableFile:root.listFiles()){
			try {
				tp.execute(new ExecThread(cnt,tableFile,month));
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
			MultiThreadRecoverySqlFile.getDataSource().destory();
		} catch (IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
	static class ExecThread implements Runnable{
		
		private AtomicInteger cnt;
		
		private File file;
		
		private String month;
		
		public ExecThread(){}
		
		public ExecThread(AtomicInteger cnt,File file,String month){
			this.cnt=cnt;
			this.file=file;
			this.month=month;
		}

		@Override
		public void run() {
			try{
				exec(file,month);
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
	
	public static void exec(File sqlFile,String month) throws Exception{
		if(sqlFile.exists()){
			File monthFile=findFileDir(sqlFile, month);
			
			if(null==monthFile)
				return;
			
			for(File execFile:monthFile.listFiles()){
				String shemaAndTable=fuckMap.get(sqlFile.getName());
				
				if(shemaAndTable.split(",")[1].equals("order_sharding"))
					continue;
				
				
				String[] params=shemaAndTable.split(",");
				
				Connection conn=null;
				PreparedStatement ps=null;
				LppzBasicDataSource baseDataSource=null;
				String loadSql="";
				try {
					baseDataSource=(LppzBasicDataSource)dataSource.getTargetDataSources().get("ds-"+params[0]);
					conn = baseDataSource.getConnection();
					ps = conn.prepareStatement(loadSql);
					
					loadSql="load data LOCAL infile '"+execFile.getAbsolutePath()+"' into table "+params[1]+" fields terminated by '\\t' lines terminated by '\\n'";
//					loadSql="load data LOCAL infile '"+execFile.getAbsolutePath().replaceAll("\\\\", "/")+"' into table "+params[1]+" fields terminated by '\\t' lines terminated by '\\n'";
					
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
		MultiThreadRecoverySqlFile.dataSource = dataSource;
	}
}
