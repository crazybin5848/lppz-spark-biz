package com.lppz.spark.oms;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import com.lppz.oms.api.model.HistOrderQueryParams;
import com.lppz.spark.oms.scala.ExportOmsHisDataHandler;
import com.lppz.spark.scala.jdbc.MysqlSpark;


public class ExportOMSDataSpark {
	
	private static Logger log = Logger.getLogger(ExportOMSDataSpark.class);
	
	public static void main(String[] args) throws Exception{
		if(args==null||args.length==0){
			String type = "ORDER";
			args = new String[] { "local[8]","omshisorderExport","omsext","hdfs://hamaster:9000",
					"{\"logisticState\":\"OUTSTORAGE\",\"orderIds\":\"WM150731000000502,WM150731000000506,GW160801013073206\",\"pageNum\":0,\"pageSize\":1000,\"queryType\":\""+type+"\"}",
//				"http://192.168.19.184:11060/services/omscache"
					"jdbc:mysql://10.8.202.231:3307/omsext"
					,"oms_ro","oms_ro",getPathName(type)};
		}
//		args = new String[] { "local[8]","omshisorderExport","omsext","hdfs://hamaster:9000","{\"logisticState\":\"OUTSTORAGE\",\"returnCodes\":\"JD160801Q13076781\",\"pageNum\":0,\"pageSize\":1000,\"queryType\":\"CANCEL\"}"};
//		args = new String[] { "local[8]","omshisorderExport","omsext","hdfs://hamaster:9000","{\"logisticState\":\"OUTSTORAGE\",\"returnCodes\":\"QJ160805T14359084\",\"pageNum\":0,\"pageSize\":1000,\"queryType\":\"RETURN\"}"};
		SparkContext sc=null;
		
		String appName = null;
		String mode = null;
		String schema = null;
		String hdfsUrl = null;
		String mysqlUrl = null;
		String mysqlUser = null;
		String mysqlPwd = null;
		String pathName = null;
		HistOrderQueryParams params = null;
		
		if (args.length == 9) {
			log.info("---------"+args[4] + "  ::  " + args[5] + " :: " + args[6] + " :: " + args[7] + " :: " + args[8]);
			mode = args[0];
			appName = args[1];
			schema = args[2];
			hdfsUrl = args[3];
			params = com.alibaba.fastjson.JSON.parseObject(args[4], HistOrderQueryParams.class);
			mysqlUrl = args[5];
			mysqlUser = args[6];
			mysqlPwd = args[7];
			pathName = args[8];
		}else{
			System.exit(-1);
		}
		
		try {
			sc = new MysqlSpark().buildSc(appName, mode);
			
			ExportOmsHisDataHandler handler = new ExportOmsHisDataHandler();
			File path = new File(pathName);
			checkDir(path);
			handler.exportOmsData(appName, params, schema, hdfsUrl, sc, mode,mysqlUrl,mysqlUser,mysqlPwd,pathName);
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}
		finally{
			if(sc!=null)
			 sc.stop();
		}
	}
	
	private static void checkDir(File f){
		if(!f.exists()){
			checkDir(f.getParentFile());
			f.mkdir();
		}
	}

	private static String getOverMark() {
		return "/tmp/exportOmsHis"+System.currentTimeMillis();
	}

	private static String getPathName(String queryType) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		return "/tmp/omsHistoryExport/"+queryType+"_"+sdf.format(new Date());
	}
}