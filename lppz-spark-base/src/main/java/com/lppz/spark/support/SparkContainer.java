package com.lppz.spark.support;

import org.apache.spark.SparkContext;

import com.lppz.spark.scala.jdbc.MysqlSpark;

public class SparkContainer {
	private SparkContext sc;
	private MysqlSpark mysql;
	public SparkContext getSc() {
		return sc;
	}
	public MysqlSpark getMysql() {
		return mysql;
	}
	public void setSc(SparkContext sc) {
		this.sc = sc;
	}
	public void setMysql(MysqlSpark mysql) {
		this.mysql = mysql;
	}
	
	
}
