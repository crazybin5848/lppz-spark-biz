package com.lppz.spark.transfer;

import java.util.List;
import java.util.Map;

import com.lppz.spark.util.SparkHiveUtil;


public class OrderSparkTransfer extends FetchSparkTransfer {
 
	@Override
	protected StringBuilder generateOutput(Map<String, Object> fetchMap, List<String> list, String tableName,
			String relateKey,StringBuilder sb) {
		return (StringBuilder) fetchMap.get("sb");
	}

	@Override
	protected String fileDir(String schema, String table, String month) {
		return new StringBuilder("/tmp/mysqldata_test/").append(schema).append("/").append(table).append("/").append(month).toString() ;
	}

	@Override
	protected String appName(String schema, String table) {
		return new StringBuilder("/tmp/mysqldata_test/").append("export mysqltable data").append(schema).append(":").append(table).append(SparkHiveUtil.now()).toString() ;
	}

}
