package com.lppz.spark.oms.bean;

import org.yaml.snakeyaml.Yaml;

public class ExportDataBean {
	private String hdfsUrl;
	private String schema;
	private String dataSql;
	private String countSql;
	private long fileLines;
	private String appName;
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getDataSql() {
		return dataSql;
	}
	public void setDataSql(String dataSql) {
		this.dataSql = dataSql;
	}
	public String getCountSql() {
		return countSql;
	}
	public void setCountSql(String countSql) {
		this.countSql = countSql;
	}
	public String getHdfsUrl() {
		return hdfsUrl;
	}
	public void setHdfsUrl(String hdfsUrl) {
		this.hdfsUrl = hdfsUrl;
	}
	public String getAppName() {
		return appName;
	}
	public void setAppName(String appName) {
		this.appName = appName;
	}
	
	public long getFileLines() {
		return fileLines;
	}
	public void setFileLines(long fileLines) {
		this.fileLines = fileLines;
	}
	public static void main(String[] args) {
		ExportDataBean bean = new ExportDataBean();
		bean.setAppName("export oms tmall member");
		bean.setHdfsUrl("hdfs://hamaster:9000");
		bean.setSchema("omsext");
		bean.setDataSql("select a.shad_mobilephone,a.orderid,a.paymentdate "
				+ "from omsextorders a  left join omsextordersharding s "
				+ "on a.orderid=s.orderid "
				+ "where "
				+ "a.basestore='single|BaseStoreData|1007' "
				+ "and paymentdate=("
				+ "select max(paymentdate) "
				+ "from omsextorders b "
				+ "where a.shad_mobilephone=b.shad_mobilephone and b.basestore='single|BaseStoreData|1007')");
//		bean.setDataSql("select shad_mobilephone,orderid,paymentdate "
//				+ "from omsextorders a  "
//				+ "where "
//				+ "a.ds='ds-' "
//				+ "and a.basestore='single|BaseStoreData|1007' "
//				+ "and paymentdate=("
//				+ "select max(paymentdate) "
//				+ "from omsextorders b "
//				+ "where a.shad_mobilephone=b.shad_mobilephone and b.ds='ds-' and b.basestore='single|BaseStoreData|1007')");
		bean.setCountSql("select max(id) from omsextordersharding");
		bean.setFileLines(1000000);
		Yaml yaml = new Yaml();
		System.out.println(yaml.dump(bean));
	}
}
