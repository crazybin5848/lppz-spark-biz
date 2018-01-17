package com.lppz.spark.accmember.bean;

import java.io.Serializable;

public class MyCatInfoBean implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4740561115185072890L;

	private String  targetJdbcUrl;
	private String targetUser;
	private String targetPwd;
	public String getTargetJdbcUrl() {
		return targetJdbcUrl;
	}
	public void setTargetJdbcUrl(String targetJdbcUrl) {
		this.targetJdbcUrl = targetJdbcUrl;
	}
	public String getTargetUser() {
		return targetUser;
	}
	public void setTargetUser(String targetUser) {
		this.targetUser = targetUser;
	}
	public String getTargetPwd() {
		return targetPwd;
	}
	public void setTargetPwd(String targetPwd) {
		this.targetPwd = targetPwd;
	}
}
