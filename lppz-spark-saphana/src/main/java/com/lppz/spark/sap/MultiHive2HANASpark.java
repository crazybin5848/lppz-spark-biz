package com.lppz.spark.sap;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import com.lppz.spark.sap.scala.Hive2RdbmsHandler;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import com.lppz.spark.sap.bean.Hive2DbmsBean;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.util.SparkHiveUtil;
import com.lppz.spark.util.SparkYamlUtils;

public class MultiHive2HANASpark {
	
	private static Logger log = Logger.getLogger(MultiHive2HANASpark.class);
	
	static boolean isTest=false;
	static boolean dataType=true;
	static boolean optimize=true;

	public static void main(String[] args) throws Exception{
//		args = new String[] { "C:\\Users\\romeo\\Desktop\\rpt_tof_cust_ind_d_spark.yaml", "local[8]","false"};
		
		if (args.length == 0)
			throw new IOException("need yaml config");
		
		Boolean isDir=Boolean.valueOf(args[2]);
		
		if(isDir){
			if(checkDirectoryExist(args[0])){
				File dir=new File(args[0]);
				
				File[] yamls=dir.listFiles(new FilenameFilter() {
					
					@Override
					public boolean accept(File dir, String name) {
						return name.endsWith(".yaml");
					}
				});
				
				if(yamls!=null && yamls.length>0){
					SparkContext sc=null;
					Hive2DbmsBean bean =null;
					for(File yaml:yamls){
						try{
							bean = SparkYamlUtils.loadYaml(yaml.getAbsolutePath(), isTest,Hive2DbmsBean.class);
							String appName = "export hive table " + bean.getHiveSchema() + ":" + bean.getHiveTableName() + SparkHiveUtil.now();
							sc = new MysqlSpark().buildSc(appName, args[1]);
							
							Hive2RdbmsHandler handler=new Hive2RdbmsHandler();
							
//							handler.handlerLoadHaNaWithJdbc(appName, bean, sc, args[3]);
							handler.genCsvForHaNaWithSql(appName, bean, sc);
							log.info("export end;");
						}catch(Exception ex){
							log.error(ex.getMessage(), ex);
						}
					}
					
					if(sc!=null){
						sc.stop();
					}
				}
			}
		}else{
			SparkContext sc=null;
			Hive2DbmsBean bean =null;
			File yaml=new File(args[0]);
			bean = SparkYamlUtils.loadYaml(yaml.getAbsolutePath(), isTest,Hive2DbmsBean.class);
			String appName = "export hive table " + bean.getHiveSchema() + ":" + bean.getHiveTableName() + SparkHiveUtil.now();
			sc = new MysqlSpark().buildSc(appName, args[1]);
			Hive2RdbmsHandler handler=new Hive2RdbmsHandler();
//			handler.handlerLoadHaNaWithJdbc(appName, bean, sc, args[3]);
			handler.genCsvForHaNaWithSql(appName, bean, sc);
			log.info("export end;");
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
