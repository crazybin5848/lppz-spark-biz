package com.lppz.spark.accmember;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.spark.sql.Row;

import com.lppz.spark.accmember.bean.TableBean;
import com.lppz.spark.util.jedis.SparkJedisCluster;

public class BuildLoadFileUtil implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4998456232962158402L;

	public String buildLoadFile(String tableName,TableBean table,HashMap<String,String> enumTable,Row r,SparkJedisCluster jedisCluster){
		String selectColumn=table.getSelectColumn().get(tableName);
	    
		String nullType="\\N";
	    String splitKey="\t";
	    
	    StringBuilder sb=new StringBuilder();
		
	    for(int j=0;j<selectColumn.split(",").length;j++){
	    	String x=selectColumn.split(",")[j];
	    	
	    	if(x.startsWith("#")){
	    		String column=x.replaceFirst("#", "");
		        int index=r.fieldIndex(column);
		        Object o=r.get(index);
		        
		        if(o==null || o.toString().equals("")){
		        	sb.append(nullType);
		        }else{
		        	String cc=enumTable.get(o.toString().trim());
		        	if(null==cc || cc.length()==0){
		        		sb.append(nullType);
		        	}else{
		        		sb.append(cc);
		        	}
		        }
	    	}else if(x.startsWith("$")){
	    		if(null==jedisCluster)
	    			throw new RuntimeException("jediscluster is null");
	    		
	    		String column=x.replaceFirst("\\$", "");
	    		String redisKey=table.getRedisKey();
	    		
	    		String key=r.get(r.fieldIndex(redisKey)).toString();
	    		
	    		byte[] value=jedisCluster.hget(key.getBytes(), column.getBytes());
				
				if(null==value){
					sb.append("0");
				}else{
					sb.append(new String(value));
				}
	    		
	    	}else{
	    		int index=r.fieldIndex(x);
		        Object o=r.get(index);
		        if (o instanceof String) {
		          if ("null".equals(o.toString().toLowerCase()))
		            o = nullType;
		          o = (o.toString()).replaceAll("\r", "");
		          o = (o.toString()).replaceAll("\n", "");
		          o = (o.toString()).replaceAll("\t", " ");
		          o = (o.toString()).replaceAll("\\\\", "\\\\\\\\");
		          o = (o.toString()).replaceAll("\"", "");
		        }
		        if (o == null || o.toString().equals(""))
		          o = nullType;
		        
		        sb.append(o);
	    	}
	    	if (j < selectColumn.split(",").length - 1)
	    		sb.append(splitKey);
	    }
	    return sb.toString();
	}
	public static void main(String[] args){
//		String aa="陈红燕\\苏文允";
//		
//		System.out.println(aa.replaceAll("\\\\", "\\\\\\\\"));
		
//		String aa="id,partner_no,member_no,card_no,#level_type,$all_scores,credit_scores,behavior_scores,$all_card_balance,card_balance,donation_amount,card_status";
//		
//		for(int j=0;j<aa.split(",").length;j++){
//			String x=aa.split(",")[j];
//			
//			if(x.startsWith("$")){
//				System.out.println(x.replaceAll("\\$", ""));
//			}
//		}
		
//		try {
//			SparkJedisCluster jedisCluster = SparkJedisClusterUtil.getJedisCluster(new FileSystemResource("F:\\workspace\\lppz-spark-biz\\lppz-spark-accmember\\src\\main\\resources\\jedis-cluster.yaml").getInputStream());
//			
//			Set<String> s=jedisCluster.keys("*");
//			
//			System.out.println(s.size());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		String aa="aa\n";
		
		System.out.print(aa.substring(0,aa.length()-1));
		
	}
}
