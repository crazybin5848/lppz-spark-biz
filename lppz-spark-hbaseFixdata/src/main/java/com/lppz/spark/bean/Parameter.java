package com.lppz.spark.bean;

import java.io.Serializable;

public class Parameter implements Serializable {

	private static final long serialVersionUID = -7060647394492425969L;

	private String day;

	private String mode;

	private String dsPath;

	private String hbasePath;

	private String rowkeyPath;

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public String getDsPath() {
		return dsPath;
	}

	public void setDsPath(String dsPath) {
		this.dsPath = dsPath;
	}

	public String getHbasePath() {
		return hbasePath;
	}

	public void setHbasePath(String hbasePath) {
		this.hbasePath = hbasePath;
	}

	public String getRowkeyPath() {
		return rowkeyPath;
	}

	public void setRowkeyPath(String rowkeyPath) {
		this.rowkeyPath = rowkeyPath;
	}

}