package com.lppz.spark.bean;

import java.io.Serializable;

public class JedisHbaseBean implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -1099139148981322051L;
	private String prefix;
	private String mainId;
	private String orgMainId;
	private String parentId;
	private String orgParentId;
	public String getOrgMainId() {
		return orgMainId;
	}
	public void setOrgMainId(String orgMainId) {
		this.orgMainId = orgMainId;
	}
	public String getOrgParentId() {
		return orgParentId;
	}
	public void setOrgParentId(String orgParentId) {
		this.orgParentId = orgParentId;
	}
	public JedisHbaseBean(){}
	public JedisHbaseBean(String prefix,String mainId,String parentId){
		this.prefix=prefix;
		this.mainId=mainId;
		this.parentId=parentId;
	}
	public String getPrefix() {
		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	public String getMainId() {
		return mainId;
	}
	public void setMainId(String mainId) {
		this.mainId = mainId;
	}
	public String getParentId() {
		return parentId;
	}
	public void setParentId(String parentId) {
		this.parentId = parentId;
	}
}
