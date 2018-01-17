package com.lppz.spark.sap;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import com.lppz.spark.sap.scala.Hive2RdbmsHandler;
import com.lppz.spark.sap.bean.Hive2DbmsBean;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.util.SparkHiveUtil;
import com.lppz.spark.util.SparkYamlUtils;

public class ExportHANADataSpark {
	
	private static Logger log = Logger.getLogger(ExportHANADataSpark.class);
	
	static boolean isTest=false;
	static boolean dataType=true;
	static boolean optimize=true;
	static int totalOnce=500000;

	public static void main(String[] args) throws Exception{
//		args = new String[] { "C:\\Users\\romeo\\Desktop\\azihyd0800.yaml", "local[8]","false",500000};
		
		if (args.length == 0)
			throw new IOException("need yaml config");
		
		isTest=Boolean.valueOf(args[2]);
		
		totalOnce=args.length==4?Integer.parseInt(args[3]):500000;
		
		SparkContext sc=null;
		Hive2DbmsBean bean =null;
		
		bean = SparkYamlUtils.loadYaml(args[0], isTest,Hive2DbmsBean.class);
		
		String appName = "export hana table " + bean.getRdbmsSchemaName() + ":" + bean.getRdbmsTableName() + SparkHiveUtil.now();
		sc = new MysqlSpark().buildSc(appName, args[1]);
		
		Hive2RdbmsHandler handler=new Hive2RdbmsHandler();
		
		String hiveMaxRowNum=handler.getHiveMaxRowNum(appName, bean, sc);
		
		if(null!=hiveMaxRowNum && hiveMaxRowNum.length()>0){
			Long jdbcMaxRowNum=fetchMax(bean);
			
			if(null==jdbcMaxRowNum || 0==jdbcMaxRowNum){
				System.exit(-1);
			}
			
			Long totalRows=jdbcMaxRowNum-Long.valueOf(hiveMaxRowNum);
			
			if(totalRows<=0)
				System.exit(0);
			
			Long page=totalRows % totalOnce==0?totalRows/totalOnce:(totalRows/totalOnce)+1;
			
			for(int i=0;i<page;i++){
				Long lowerBound=Long.valueOf(hiveMaxRowNum)+i*totalOnce;
				Long upperBound=lowerBound+totalOnce>jdbcMaxRowNum?jdbcMaxRowNum:lowerBound+totalOnce;
				
				handler.exportHaNaTable(appName, bean, sc, lowerBound, upperBound);
			}
			
		}else{
			System.exit(-1);
		}
	}
	
	private static Long fetchMax(Hive2DbmsBean bean) throws Exception {
		Connection conn = null;
		String sql = "select max(rownumber) from "+bean.getRdbmsSchemaName()+"."+"\""+bean.getRdbmsTableName()+"\"";
		String url = bean.getRdbmsJdbcUrl();
		try {
			Class.forName(bean.getRdbmsdbDriver());
			conn = DriverManager.getConnection(url, bean
					.getRdbmsJdbcUser(), bean.getRdbmsJdbcPasswd());
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while(rs.next()) {
				return rs.getLong(1);
			}			
			return null;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
			}
		}
	}

}
