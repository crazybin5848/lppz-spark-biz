package com.lppz.spark.bean;

import java.io.Serializable;
import java.util.Map;

public class HBaseMeta implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -11231231565653L;
	public HBaseMeta(String regionNum,Map<String, String> regionMap){
		this.regionNum=regionNum;
		this.regionMap=regionMap;
	}
	public HBaseMeta(){
	}
	
	private String regionNum;
	private Map<String, String> regionMap;
	public String getRegionNum() {
		return regionNum;
	}

	public void setRegionNum(String regionNum) {
		this.regionNum = regionNum;
	}

	public Map<String, String> getRegionMap() {
		return regionMap;
	}

	public void setRegionMap(Map<String, String> regionMap) {
		this.regionMap = regionMap;
	}
}