package com.lppz.spark;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import com.lppz.spark.bean.SparkHiveSqlBean;
import com.lppz.spark.scala.HistoryOrderHandler;
import com.lppz.spark.scala.SparkHdfsUtil;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.support.TransferDate;
import com.lppz.spark.transfer.MainSparkTransfer;
import com.lppz.spark.transfer.ParseNewSparkTransfer;
import com.lppz.spark.util.SparkHiveUtil;
import com.lppz.spark.util.SparkYamlUtils;

public class MultiExportDataSpark {
	
	private static Logger log = Logger.getLogger(MultiExportDataSpark.class);
	
	static boolean isTest=false;
	static boolean dataType=true;
	static boolean optimize=true;

	public static void main(String[] args) throws Exception{
//		args = new String[] { "F:\\o2ooms\\", "local[1]", "month,;2016-08,;maxdate,;'2016-08-02',;mindate,;'2016-08-01',;span,;0,;total4Once,;50000,;dataType,;false" };
		
		if (args.length == 0)
			throw new IOException("need yaml config");
		String[] params = args[args.length - 1].split(",;");
		String month = null;
		Long total4Once = null;
		for(int i=0;i<params.length;){
			if("month".equals(params[i]))
				month = params[i+1];
			if ("total4Once".equals(params[i]))
				total4Once = Long.valueOf(params[i + 1]);
			if ("dataType".equals(params[i]))
				dataType = Boolean.valueOf(params[i + 1]);
			if ("optimize".equals(params[i]))
				optimize = Boolean.valueOf(params[i + 1]);
			i+=2;
		}
		if(month ==null)
			throw new IOException("need month config");
		
		if(checkDirectoryExist(args[0])){
			File dic=new File(args[0]);
			
			File[] yamls=dic.listFiles(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".yaml");
				}
			});
			
			ExportAllOrderDataSpark.dataType=dataType;
			ExportAllOrderDataSpark.isTest=isTest;
			
			if(yamls!=null && yamls.length>0){
				ExportAllOrderDataSpark eaod=new ExportAllOrderDataSpark();
				SparkContext sc=null;
				SparkHiveSqlBean bean =null;
				for(File yaml:yamls){
					try{
						bean = SparkYamlUtils.loadYaml(yaml.getAbsolutePath(), isTest);
						if (total4Once != null && total4Once != 0)
							bean.getMysqlBean().setTotal4Once(total4Once);
						String mode = args.length == 1 ? "local" : args[1];
						MainSparkTransfer.buildMonthInterval(args);
						TransferDate tfd = new TransferDate().buildDate(params, month);
						if(tfd.getMaxdate()==null)
							throw new IOException("need maxdate");
						ParseNewSparkTransfer.parseAll(bean, args);
						HistoryOrderHandler mysql = new HistoryOrderHandler();
						String appName = "export all" + bean.getConfigBean().getSchema() + ":" + bean.getMysqlBean().getTableName() + SparkHiveUtil.now();
						sc = new MysqlSpark().buildSc(appName, mode);
						eaod.dofetchMysqlFromSparkToLocal(mysql,sc,bean.getConfigBean(), bean.getMysqlBean(), mode, bean.getSparkMapbean()==null?null:bean.getSparkMapbean(),
								month,tfd,bean.getSourcebean());
						log.info("fetch data end;");
					}catch(Exception ex){
						log.error(ex.getMessage(), ex);
					}
				}
				
				if(sc!=null){
					sc.stop();
					FileSystem fs=new SparkHdfsUtil().getFileSystem(bean.getSourcebean().getHdfsUrl());
					fs.delete(new Path("/tmp/mysqlhive"), true);
					fs.close();
				}
			}
		}
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
