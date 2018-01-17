package com.lppz.spark.mysql;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;

import com.lppz.spark.mysql.bean.Hive2DbmsBean;
import com.lppz.spark.mysql.scala.Hive2RdbmsHandler;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.util.SparkHiveUtil;
import com.lppz.spark.util.SparkYamlUtils;

public class MultiHive2DbmsSpark {
	
	private static Logger log = Logger.getLogger(MultiHive2DbmsSpark.class);
	
	static boolean isTest=false;
	static boolean dataType=true;
	static boolean optimize=true;

	public static void main(String[] args) throws Exception{
//		args = new String[] { "C:\\Users\\romeo\\Desktop\\azihyd0800.yaml", "local[8]","false","maxdate,;'2015-08-02',;mindate,;'2015-08-01'"};
		
		if (args.length == 0)
			throw new IOException("need yaml config");
		
		Boolean isDir=Boolean.valueOf(args[2]);
		
		String[] params = args[args.length - 1].split(",;");
		
		String maxDate=null;
		String minDate=null;
		
		for(int i=0;i<params.length;){
			if("maxdate".equals(params[i]))
				maxDate = params[i+1];
			if ("mindate".equals(params[i]))
				minDate = params[i+1];
			i+=2;
		}
		
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
							
							if(null!=maxDate && null!=minDate && null!=bean){
								recoverYamlSql(minDate,maxDate,bean);
							}
							
							String appName = "export hive table " + bean.getHiveSchema() + ":" + bean.getHiveTableName() + SparkHiveUtil.now();
							sc = new MysqlSpark().buildSc(appName, args[1]);
							
							Hive2RdbmsHandler handler=new Hive2RdbmsHandler();
							if(bean.getUseSql()){
								
								if(bean.getRdbmsdbDriver().equals("com.sap.db.jdbc.Driver")){
									handler.genCsvForHaNaWithSql(appName, bean, sc);
								}else{
									Row[] arrayRows=handler.loadWithJdbc(appName, bean, sc);
									
									execJdbcOper(arrayRows,bean);
								}
							}else{
								if(bean.getRdbmsdbDriver().equals("com.sap.db.jdbc.Driver")){
									handler.genCsvForHaNaWithHivePath(appName, bean, sc);
								}else{
									List<String> list=handler.buildMysqlLoadFile(appName, bean, sc);
									
									execJdbcOper(list,bean);
								}
							}
							
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
			
			if(null!=maxDate && null!=minDate && null!=bean){
				recoverYamlSql(minDate,maxDate,bean);
			}
			
			String appName = "export hive table " + bean.getHiveSchema() + ":" + bean.getHiveTableName() + SparkHiveUtil.now();
			sc = new MysqlSpark().buildSc(appName, args[1]);
			Hive2RdbmsHandler handler=new Hive2RdbmsHandler();
			if(bean.getUseSql()){
				if(bean.getRdbmsdbDriver().equals("com.sap.db.jdbc.Driver")){
					handler.genCsvForHaNaWithSql(appName, bean, sc);
				}else{
					Row[] arrayRows=handler.loadWithJdbc(appName, bean, sc);
					
					execJdbcOper(arrayRows,bean);
				}
			}else{
				if(bean.getRdbmsdbDriver().equals("com.sap.db.jdbc.Driver")){
					handler.genCsvForHaNaWithHivePath(appName, bean, sc);
				}else{
					List<String> list=handler.buildMysqlLoadFile(appName, bean, sc);
					
					execJdbcOper(list,bean);
				}
			}
			
			log.info("export end;");
		}
	}
	
	private static void recoverYamlSql(String minDate, String maxDate, Hive2DbmsBean bean) {
		String sql=bean.getHiveSql();
		
		if(null==sql || "".equals(sql.trim()))
			return;
		
		sql=sql.replaceAll("#mindate#", minDate).replaceAll("#maxdate#", maxDate);
		
		bean.setHiveSql(sql);
	}

	private static void execJdbcOper(List<String> list,Hive2DbmsBean bean){
		if(null==list || list.size()==0)
			return;
		
		int total =list.size();
	    
	    int pageSize=50000;
	    
	    int totalPage=total%pageSize==0?total/pageSize : (total/pageSize)+1;
	    
	    Connection conn=null;
	    Statement stmt=null;
	    try {
			Class.forName(bean.getRdbmsdbDriver());
			conn = DriverManager.getConnection(bean.getRdbmsJdbcUrl(), bean.getRdbmsJdbcUser(), bean.getRdbmsJdbcPasswd());
			stmt = conn.createStatement();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} 
	    
	    for(int i=0;i<totalPage;i++){
	    	List<String> tmp=list.subList(i*pageSize, (i+1)*pageSize>total?total:(i+1)*pageSize);
	    	
	    	String sql=buildFileSql(tmp,bean);
	    	
	    	if(sql!=null){
	    		try {
					stmt.execute(sql);
				} catch (SQLException e) {
					e.printStackTrace();
				}
	    	}
	    		
	    }
	    
	    try{
	    	if(conn!=null)
	    		conn.close();
	    	
	    	if(stmt!=null)
	    		stmt.close();
	    }catch(Exception ex){
	    	
	    }
	}
	
	private static void execJdbcOper(Row[] rows,Hive2DbmsBean bean){
		if(null==rows || rows.length==0)
			return;
		
		List<Row> list=Arrays.asList(rows);
		
	    int total =list.size();
	    
	    int pageSize=50000;
	    
	    int totalPage=total%pageSize==0?total/pageSize : (total/pageSize)+1;
	    
	    Connection conn=null;
	    Statement stmt=null;
	    try {
			Class.forName(bean.getRdbmsdbDriver());
			conn = DriverManager.getConnection(bean.getRdbmsJdbcUrl(), bean.getRdbmsJdbcUser(), bean.getRdbmsJdbcPasswd());
			stmt = conn.createStatement();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} 
	    
	    for(int i=0;i<totalPage;i++){
	    	List<Row> tmp=list.subList(i*pageSize, (i+1)*pageSize>total?total:(i+1)*pageSize);
	    	
	    	String sql=buildSql(tmp,bean);
	    	
	    	if(sql!=null){
	    		try {
					stmt.execute(sql);
				} catch (SQLException e) {
					e.printStackTrace();
				}
	    	}
	    		
	    }
	    
	    try{
	    	if(conn!=null)
	    		conn.close();
	    	
	    	if(stmt!=null)
	    		stmt.close();
	    }catch(Exception ex){
	    	
	    }
	}
	
	private static String buildFileSql(List<String> list,Hive2DbmsBean bean){
		if(list!=null && list.size()>0){
			StringBuilder sb=new StringBuilder("insert into ");
			sb.append(bean.getRdbmsTableName()).append(" values ");
			
			for(int j=0;j<list.size();j++){
				if(j==0){
					sb.append(list.get(j));
				}else{
					sb.append(",").append(list.get(j));
				}
			}
			return sb.toString();
		}
		return null;
	}
	
	private static String buildSql(List<Row> list,Hive2DbmsBean bean){
		if(list!=null && list.size()>0){
			StringBuilder sb=new StringBuilder("insert into ");
			sb.append(bean.getRdbmsTableName()).append(" values ");
			
			for(int j=0;j<list.size();j++){
				if(j==0){
					sb.append("(");
				}else{
					sb.append(",(");
				}
				
				Row row=list.get(j);
				
				for(int i=0;i<row.size();i++){
					if(i==0){
//						sb.append(row.get(i).equals("NULL")?"null":"'".concat(row.get(i).toString()).concat("'"));
						
						if(null==row.get(i) || "NULL".equalsIgnoreCase(row.get(i).toString())){
							sb.append("null");
						}else{
							sb.append("'".concat(row.get(i).toString()).concat("'"));
						}
					}else{
//						sb.append(",'").append(row.get(i).equals("NULL")?"null":row.get(i)+"'");
						if(null==row.get(i) || "NULL".equalsIgnoreCase(row.get(i).toString())){
							sb.append(",null");
						}else{
							sb.append(",'".concat(row.get(i).toString()).concat("'"));
						}
					}
				}
				sb.append(")");
			}
			return sb.toString();
		}
		return null;
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
