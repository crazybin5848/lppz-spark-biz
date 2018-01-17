package com.lppz.spark.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;


public class OmsBaseStoreUtil {
	private static Logger logger = LoggerFactory.getLogger(OmsBaseStoreUtil.class);
	static Properties props = null;
	
	public static void loadResource(){
		String path = System.getProperty("OmsJdbcFilePath");
		File file = new File(path);
		try {
			Resource resource = new InputStreamResource(new FileInputStream(file));
			props = PropertiesLoaderUtils.loadProperties(resource);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} 
	}
	
	public static Map<String,String> getAllIdCode(){
		String url = getKey("jdbc.url");
		String username = getKey("jdbc.user");
		String password = getKey("jdbc.passwd");
		Map<String,String> storeIdMap = new HashMap<String, String>();
		String sql = "select id,`code` from basestores";
		String tmpId = null;
		try {
			List<Map<String,Object>> resultList = exeucteQuery(url, username, password, sql);
			if (CollectionUtils.isNotEmpty(resultList)) {
				for (Map<String,Object> map : resultList) {
					tmpId = (String)map.get("id");
						storeIdMap.put(tmpId.replaceAll("\\|", "!!"), (String)map.get("code"));
				}
			}
		} catch (SQLException e) {
			logger.error("read oms basetore fail ",e);
		}
		return storeIdMap;
	}
	
	public static String getKey(final String keystr)
	{
		if (props == null) {
			loadResource();
		}
			return props.getProperty(keystr);
	}
	
	public static Connection getSqlConnection(String url, String username, String password){
		Connection con = null;
		try {
		      Class.forName("com.mysql.jdbc.Driver");
		      con = DriverManager.getConnection(url, username, password);
		    } catch (ClassNotFoundException | SQLException e) {
		      throw new RuntimeException(e);
		}
		return con;
	}
	
	public static List<Map<String,Object>> exeucteQuery(String url, String username, String password, String sql) throws SQLException{
		
		Connection con = null;
		Statement stmt = null;
		List<Map<String,Object>> list = new ArrayList<>();
		try {
			con = getSqlConnection(url, username, password);
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			ResultSetMetaData md = rs.getMetaData(); //得到结果集(rs)的结构信息，比如字段数、字段名等 
			int columnCount = md.getColumnCount(); //返回此 ResultSet 对象中的列数
			Map<String,Object> rowData = null;
			while (rs.next()) {   
				rowData = new HashMap<>(columnCount);   
				for (int i = 1; i <= columnCount; i++) {   
					rowData.put(md.getColumnName(i), rs.getObject(i));   
				}   
				list.add(rowData);   
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}finally{
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e2) {
				}
			}
			
			if (con != null) {
				try {
					con.close();
				} catch (Exception e2) {
				}
			}
		}
		return list;
	}

}
