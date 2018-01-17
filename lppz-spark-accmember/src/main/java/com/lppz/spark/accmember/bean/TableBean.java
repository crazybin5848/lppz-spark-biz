package com.lppz.spark.accmember.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class TableBean implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2232472020701042986L;

	private Integer batchSize;
	private Integer partition;
	
	private String primaryKey;
	private String seqTable;
	private String sourceTableOrView;
	private ArrayList<String> targetColumns;
	private ArrayList<String> targetTableName;
	private HashMap<String,ArrayList<String>> joinKey;
	private HashMap<String,String> selectColumn;
	private String redisKey;
	
	public String getRedisKey() {
		return redisKey;
	}
	public void setRedisKey(String redisKey) {
		this.redisKey = redisKey;
	}
	public HashMap<String, String> getSelectColumn() {
		return selectColumn;
	}
	public void setSelectColumn(HashMap<String, String> selectColumn) {
		this.selectColumn = selectColumn;
	}
	public Integer getPartition() {
		return partition;
	}
	public void setPartition(Integer partition) {
		this.partition = partition;
	}
	public String getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	public HashMap<String,ArrayList<String>> getJoinKey() {
		return joinKey;
	}
	public void setJoinKey(HashMap<String,ArrayList<String>> joinKey) {
		this.joinKey = joinKey;
	}
	public Integer getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}
	public String getSeqTable() {
		return seqTable;
	}
	public void setSeqTable(String seqTable) {
		this.seqTable = seqTable;
	}
	public String getSourceTableOrView() {
		return sourceTableOrView;
	}
	public void setSourceTableOrView(String sourceTableOrView) {
		this.sourceTableOrView = sourceTableOrView;
	}
	public ArrayList<String> getTargetColumns() {
		return targetColumns;
	}
	public void setTargetColumns(ArrayList<String> targetColumns) {
		this.targetColumns = targetColumns;
	}
	public ArrayList<String> getTargetTableName() {
		return targetTableName;
	}
	public void setTargetTableName(ArrayList<String> targetTableName) {
		this.targetTableName = targetTableName;
	}
}
