package com.lppz.spark.oms;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import com.lppz.spark.oms.bean.ExportBean;
import com.lppz.spark.sap.scala.ExportOmsDataHandler;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.util.SparkYamlUtils;


public class ExportOMSMobileDataSpark {
	
	private static Logger log = Logger.getLogger(ExportOMSMobileDataSpark.class);
	
	static boolean isTest=false;
	static boolean dataType=true;
	static boolean optimize=true;
	static int totalOnce=5000000;

	public static void main(String[] args) throws Exception{
		if(args==null||args.length==0)
		args = new String[] { "export_oms_member.yaml","local[8]"};
		SparkContext sc=null;
		boolean isFristLoad = true;
		
		String ds = null;
		if (args.length == 3) {
			ds = args[2];
			isFristLoad = false;
		}
		
		ExportBean bean = null;
		try {
			bean = SparkYamlUtils.loadYaml(args[0], false, ExportBean.class);
		} catch (Exception e) {
			System.exit(-1);
		}
		try {
			totalOnce = bean.getTotalOnce();
			String appName = bean.getAppName();
			sc = new MysqlSpark().buildSc(bean.getAppName(), args[1]);
			
			ExportOmsDataHandler handler = new ExportOmsDataHandler();
			if (isFristLoad) {
				long maxid=handler.getMaxOmsOrderShardingId(appName,bean.getSchema(), bean.getCountSql(), sc);
				
				if( 0==maxid){
					System.exit(-1);
				}
				
				Long page=(long) (maxid % totalOnce==0?maxid/totalOnce:(maxid/totalOnce)+1);
				
				for(int i=0;i<page;i++){
					long lowerBound=(long) (i*totalOnce);
					long upperBound=lowerBound+totalOnce>maxid?maxid:lowerBound+totalOnce;
					
					handler.exportOmsMoblie(appName,bean, sc,isFristLoad,null,lowerBound,upperBound);
				}
			}else{
				handler.exportOmsMoblie(appName,bean, sc,isFristLoad,ds,0L,0L);			
			}
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}
		finally{
			if(sc!=null)
			 sc.stop();
		}
	}
}