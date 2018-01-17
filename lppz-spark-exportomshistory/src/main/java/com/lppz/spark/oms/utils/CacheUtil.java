package com.lppz.spark.oms.utils;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheUtil implements Serializable{
	private static final long serialVersionUID = 8339422665369289347L;
	private static final Logger logger = LoggerFactory.getLogger(CacheUtil.class);
	public static final String KEY_BASESTORE = "bastore";
	public static final String KEY_PRODUCT = "product";
	public static final String KEY_LOCATION = "location";
	public static final String KEY_AREA = "area";
	private Map<String,Map<String,String>> cacheMap = new HashMap<>();
	
	private Connection conn;
	
	public CacheUtil(String mysqlUrl, String user, String pwd) {
		   String driver = "com.mysql.jdbc.Driver";
		    String url = mysqlUrl;
		    String username = user;
		    String password = pwd;
		    try {
		        Class.forName(driver); //classLoader,加载对应驱动
		        conn = (Connection) DriverManager.getConnection(url, username, password);
		        initCache();
		    } catch (ClassNotFoundException e) {
		        e.printStackTrace();
		    } catch (SQLException e) {
		        e.printStackTrace();
		    }
	}
	
	public Map<String,Map<String,String>> getCacheMap(){
		return cacheMap;
	}
	
	public void initCache(){
		if (conn != null) {
			initBaseStoreCache();
			initAreaCache();
			initProductCache();
			initLocationCache();
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("close mysql connection exception",e);
			}
		}
	}
	
	private Map<String,String> executeSelect(String sql,String key,String value){
		Map<String,String> resultMap = new HashMap<>();
		  PreparedStatement pstmt;
		    try {
		        pstmt = (PreparedStatement)conn.prepareStatement(sql);
		        ResultSet rs = pstmt.executeQuery();
		        int columnNum = rs.getMetaData().getColumnCount();
		        while (rs.next()) {
		        	resultMap.put(rs.getString(key), rs.getString(value));
		        }
		    } catch (SQLException e) {
		        logger.error("execute sql exception",e);
		    }
		return resultMap;
	}
	
	private void initBaseStoreCache() {
		String sql = "select id,`name` from basestores";
		cacheMap.put(KEY_BASESTORE, executeSelect(sql,"id","name"));
	}

	private void initLocationCache() {
		String sql = "select locationid,storename from stockroomlocations ";
		cacheMap.put(KEY_LOCATION, executeSelect(sql,"locationid","storename"));
	}

	private void initProductCache() {
		String sql = "select productid,`name` from busi_product_data";
		cacheMap.put(KEY_PRODUCT, executeSelect(sql,"productid","name"));
		
	}

	private void initAreaCache() {
		String sql = "select `code`,`name` from busi_area_data";
		cacheMap.put(KEY_AREA, executeSelect(sql,"code","name"));
	}
}
