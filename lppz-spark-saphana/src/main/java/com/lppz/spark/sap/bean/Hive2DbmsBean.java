package com.lppz.spark.sap.bean;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;

import org.yaml.snakeyaml.Yaml;

public class Hive2DbmsBean implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -9208293735763983537L;
	private String hiveSchema;
	private String hiveTableName;
	private String rdbmsTableName;
	private String rdbmsSchemaName;
	private String rdbmsdbDriver;
	private String rdbmsJdbcUrl;
	private String rdbmsJdbcUser;
	private String rdbmsJdbcPasswd;
	private String hdfsUrl;
	private String hiveSql;
	private String hiveUser;
	private String hivePasswd;
	private Boolean useSql;
	private String hiveUrl;
	private String hiveColumns;
	private String rdbmsColumns;
	private String primaryKey;
	private String partitionColumn;
	private Long totalOnce;
	//分堆的列名
	private Integer partition;
	//是否分堆处理
	private Boolean usePart;
	
	public Boolean getUsePart() {
		return usePart;
	}
	public void setUsePart(Boolean usePart) {
		this.usePart = usePart;
	}
	public String getPartitionColumn() {
		return partitionColumn;
	}
	public void setPartitionColumn(String partitionColumn) {
		this.partitionColumn = partitionColumn;
	}
	public Integer getPartition() {
		return partition;
	}
	public void setPartition(Integer partition) {
		this.partition = partition;
	}
	public Long getTotalOnce() {
		return totalOnce;
	}
	public void setTotalOnce(Long totalOnce) {
		this.totalOnce = totalOnce;
	}
	public String getHiveColumns() {
		return hiveColumns;
	}
	public void setHiveColumns(String hiveColumns) {
		this.hiveColumns = hiveColumns;
	}
	public String getRdbmsColumns() {
		return rdbmsColumns;
	}
	public void setRdbmsColumns(String rdbmsColumns) {
		this.rdbmsColumns = rdbmsColumns;
	}
	public String getHiveUrl() {
		return hiveUrl;
	}
	public void setHiveUrl(String hiveUrl) {
		this.hiveUrl = hiveUrl;
	}
	public Boolean getUseSql() {
		return useSql;
	}
	public void setUseSql(Boolean useSql) {
		this.useSql = useSql;
	}
	public String getHiveSql() {
		return hiveSql;
	}
	public void setHiveSql(String hiveSql) {
		this.hiveSql = hiveSql;
	}
	public String getHdfsUrl() {
		return hdfsUrl;
	}
	public void setHdfsUrl(String hdfsUrl) {
		this.hdfsUrl = hdfsUrl;
	}
	public String getHiveSchema() {
		return hiveSchema;
	}
	public void setHiveSchema(String hiveSchema) {
		this.hiveSchema = hiveSchema;
	}
	public String getHiveTableName() {
		return hiveTableName;
	}
	public void setHiveTableName(String hiveTableName) {
		this.hiveTableName = hiveTableName;
	}
	public String getRdbmsTableName() {
		return rdbmsTableName;
	}
	public void setRdbmsTableName(String rdbmsTableName) {
		this.rdbmsTableName = rdbmsTableName;
	}
	public String getRdbmsSchemaName() {
		return rdbmsSchemaName;
	}
	public void setRdbmsSchemaName(String rdbmsSchemaName) {
		this.rdbmsSchemaName = rdbmsSchemaName;
	}
	public String getRdbmsdbDriver() {
		return rdbmsdbDriver;
	}
	public void setRdbmsdbDriver(String rdbmsdbDriver) {
		this.rdbmsdbDriver = rdbmsdbDriver;
	}
	public String getRdbmsJdbcUrl() {
		return rdbmsJdbcUrl;
	}
	public void setRdbmsJdbcUrl(String rdbmsJdbcUrl) {
		this.rdbmsJdbcUrl = rdbmsJdbcUrl;
	}
	public String getRdbmsJdbcUser() {
		return rdbmsJdbcUser;
	}
	public void setRdbmsJdbcUser(String rdbmsJdbcUser) {
		this.rdbmsJdbcUser = rdbmsJdbcUser;
	}
	public String getRdbmsJdbcPasswd() {
		return rdbmsJdbcPasswd;
	}
	public void setRdbmsJdbcPasswd(String rdbmsJdbcPasswd) {
		this.rdbmsJdbcPasswd = rdbmsJdbcPasswd;
	}
	
	public String getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	public String getHiveUser() {
		return hiveUser;
	}
	public void setHiveUser(String hiveUser) {
		this.hiveUser = hiveUser;
	}
	public String getHivePasswd() {
		return hivePasswd;
	}
	public void setHivePasswd(String hivePasswd) {
		this.hivePasswd = hivePasswd;
	}
	public static void main(String[] args)throws Exception{
//		Hive2DbmsBean bean=new Hive2DbmsBean();
//		
//		bean.setHdfsUrl("hdfs://hamaster:9000");
//		bean.setHiveSchema("omsext");
//		bean.setHiveTableName("omsedbbusilackorder");
//		bean.setRdbmsdbDriver("com.mysql.jdbc.Driver");
//		bean.setRdbmsJdbcPasswd("KTqHDMg8r3q1w");
//		bean.setRdbmsJdbcUrl("jdbc:mysql://192.168.37.246:3306/joblppz?useUnicode=true&characterEncoding=UTF8&tinyInt1isBit=false");
//		bean.setRdbmsJdbcUser("root");
//		bean.setRdbmsSchemaName("joblppz");
//		bean.setRdbmsTableName("busilackorder");
//		bean.setHiveSql("select `id`,`orderid`,`createtime`,`tag`,`motifytime` from `omsedbbusilackorder`");
//		bean.setUseSql(false);
//		bean.setHiveColumns("aaaaa,bbbbb");
//		bean.setRdbmsColumns("ccccccc,dddddddd");
		Yaml y=new Yaml();
		
//		System.out.println(y.dump(bean));
		
		File file=new File("C:\\Users\\romeo\\Desktop\\pzshopnew.yaml");
		
		Hive2DbmsBean bean=y.loadAs(new FileInputStream(file), Hive2DbmsBean.class);
		
		System.out.println(y.dump(bean));
	}
}
