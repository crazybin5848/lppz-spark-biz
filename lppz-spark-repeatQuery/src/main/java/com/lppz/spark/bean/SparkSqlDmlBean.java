package com.lppz.spark.bean;

import redis.clients.jedis.Jedis;

import com.lppz.spark.scala.hbase.HbaseAbstractSparkConv;


public class SparkSqlDmlBean {
	private String tableName;
	private String schema;
	private String relateKey;
	private Long total4Once;
	private String sql;
	private String colList;
	private String mainIdColumn;
	private String parentFamilyName;
	private String excludeColumnList;
	private String uidList;
	private Boolean isRootHbase;
	private Boolean isLeaf;
	private String familyName;
	
	public HbaseAbstractSparkConv buildConverter(String hbaseQuorum,String hbasePort,Jedis jedis){
		HbaseAbstractSparkConv converter=new HBaseSparkConvertor(isRootHbase,mainIdColumn,isLeaf,tableName
				,familyName,parentFamilyName,relateKey,colList,hbaseQuorum,hbasePort,jedis);
		return converter;
	}
	
	public String getMainIdColumn() {
		return mainIdColumn;
	}

	public void setMainIdColumn(String mainIdColumn) {
		this.mainIdColumn = mainIdColumn;
	}

	public Boolean getIsRootHbase() {
		return isRootHbase;
	}

	public void setIsRootHbase(Boolean isRootHbase) {
		this.isRootHbase = isRootHbase;
	}

	public Boolean getIsLeaf() {
		return isLeaf;
	}

	public void setIsLeaf(Boolean isLeaf) {
		this.isLeaf = isLeaf;
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Long getTotal4Once() {
		return total4Once;
	}

	public void setTotal4Once(Long total4Once) {
		this.total4Once = total4Once;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getRelateKey() {
		return relateKey;
	}

	public void setRelateKey(String relateKey) {
		this.relateKey = relateKey;
	}

	public String getColList() {
		return colList;
	}

	public void setColList(String colList) {
		this.colList = colList;
	}


	public String getSchema() {
		return schema;
	}


	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getParentFamilyName() {
		return parentFamilyName;
	}

	public void setParentFamilyName(String parentFamilyName) {
		this.parentFamilyName = parentFamilyName;
	}

	public String getExcludeColumnList() {
		return excludeColumnList;
	}

	public void setExcludeColumnList(String excludeColumnList) {
		this.excludeColumnList = excludeColumnList;
	}

	public String getUidList() {
		return uidList;
	}

	public void setUidList(String uidList) {
		this.uidList = uidList;
	}
}