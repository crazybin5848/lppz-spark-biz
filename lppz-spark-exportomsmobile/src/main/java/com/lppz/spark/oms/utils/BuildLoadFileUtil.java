package com.lppz.spark.oms.utils;

import java.io.Serializable;

import org.apache.spark.sql.Row;

public class BuildLoadFileUtil implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4998456232962158402L;

	public String buildLoadFile(Row r){
	    
		String nullType="\\N";
	    String splitKey="\t";
	    StringBuilder sb=new StringBuilder();
		int length = r.length();
	    for(int j=0;j<length;j++){
	    	Object o=r.get(j);
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
		        if (j < length - 1)
		    		sb.append(splitKey);
	    }
	    return sb.toString();
	}
}
