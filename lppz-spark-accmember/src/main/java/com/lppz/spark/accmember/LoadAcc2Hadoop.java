package com.lppz.spark.accmember;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.spark.SparkContext;

import com.lppz.spark.accmember.scala.LoadAcc2HadoopHandler;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.util.SparkHiveUtil;

public class LoadAcc2Hadoop {

	static Long totalOnce=2500000l;
	public static void main(String[] args){
//		args=new String[]{"jdbc:mysql://10.8.202.222:3316/member_center?useConfigs=maxPerformance",
//				"local[8]","user,;memro,;pwd,;memro222,;table,;lp_member,;port,;3316"};
		String[] params = args[args.length - 1].split(",;");
		
		String url=args[0];
		String user=null;
		String pwd=null;
		String port=null;
		String table=null;
		
		for(int i=0;i<params.length;){
			if("user".equals(params[i]))
				user = params[i+1];
			if("pwd".equals(params[i]))
				pwd = params[i+1];
			if("port".equals(params[i]))
				port = params[i+1];
			if("table".equals(params[i]))
				table = params[i+1];
			i+=2;
		}
		
		SparkContext sc=null;
		String appName = "export data  " + url + ":" + SparkHiveUtil.now();
		sc = new MysqlSpark().buildSc(appName, args[1]);
//		sc.conf().set("spark.sql.warehouse.dir", "/spark-warehouse/");
		//查询表中最大最小id，确定查询范围
		HashMap<String,Long> map=fetchMaxAndMin(url,user,pwd,table);
		LoadAcc2HadoopHandler handler=new LoadAcc2HadoopHandler();
		
		long k=0;
		long max=map.get("max");
		long min=map.get("min");
		//固定条数分片处理
		for (long i = min; i <= max; i += totalOnce+1) {
			k=totalOnce + i>max?max:totalOnce + i;
			handler.load(sc, url, user, pwd, table, port,k,i);
		}
		
		
	}
	
	private static HashMap<String,Long> fetchMaxAndMin(String url,String user,String pwd,String table){
		HashMap<String,Long> rtnMap=new HashMap<>();
		
		Connection conn = null;
		String sql = "select min(_id),max(_id) from "+table;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, user,
					pwd);
			PreparedStatement stmt = conn.prepareStatement(sql);
			
			ResultSet rs = stmt.executeQuery();
			
			while (rs.next()) {
				rtnMap.put("min", rs.getLong(1));
				rtnMap.put("max", rs.getLong(2));
			}
			return rtnMap;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(conn!=null){
					conn.close();
				}
			} catch (SQLException e) {
			}
		}
		
		return null;
	}
}
