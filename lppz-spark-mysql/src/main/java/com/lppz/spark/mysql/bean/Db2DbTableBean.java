package com.lppz.spark.mysql.bean;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.List;

import org.yaml.snakeyaml.Yaml;

public class Db2DbTableBean implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -9208293735763983537L;
	private String sourceSql;
	private String targetSql;
	private String targetTableName;
	private String targetColumns;
	private String primaryKey;
	private String parentKey;
	private Long totalOnce;
	private Integer partition;
	private String startTag;
	private String endTag;
	private Integer batchSize;
	private String sourceTableOrView;
	private String seqTable;
//	private String index
	private List<Db2DbTableBean> subBeans;
	

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

	public String getSourceSql() {
		return sourceSql;
	}

	public void setSourceSql(String sourceSql) {
		this.sourceSql = sourceSql;
	}

	public String getTargetSql() {
		return targetSql;
	}

	public void setTargetSql(String targetSql) {
		this.targetSql = targetSql;
	}

	public String getTargetColumns() {
		return targetColumns;
	}

	public void setTargetColumns(String targetColumns) {
		this.targetColumns = targetColumns;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getParentKey() {
		return parentKey;
	}

	public void setParentKey(String parentKey) {
		this.parentKey = parentKey;
	}

	public Long getTotalOnce() {
		return totalOnce;
	}

	public void setTotalOnce(Long totalOnce) {
		this.totalOnce = totalOnce;
	}

	public Integer getPartition() {
		return partition;
	}

	public void setPartition(Integer partition) {
		this.partition = partition;
	}

	public List<Db2DbTableBean> getSubBeans() {
		return subBeans;
	}

	public void setSubBeans(List<Db2DbTableBean> subBeans) {
		this.subBeans = subBeans;
	}
	
	public String getStartTag() {
		return startTag;
	}

	public void setStartTag(String startTag) {
		this.startTag = startTag;
	}

	public String getEndTag() {
		return endTag;
	}

	public void setEndTag(String endTag) {
		this.endTag = endTag;
	}

	public String getTargetTableName() {
		return targetTableName;
	}

	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	public static void main(String[] args)throws Exception{
		
		Yaml y=new Yaml();
		
//		System.out.println(y.dump(bean));
		
		File file=new File("C:\\Users\\romeo\\Desktop\\pzshopnew.yaml");
		
		Db2DbTableBean bean=y.loadAs(new FileInputStream(file), Db2DbTableBean.class);
		
		System.out.println(y.dump(bean));
	}
}
