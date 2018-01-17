package com.lppz.spark.transfer;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.lppz.spark.util.SparkHiveUtil;

public class DeleteSparkTransfer extends FetchSparkTransfer{
	@SuppressWarnings("unchecked")
	@Override
	protected StringBuilder generateOutput(Map<String, Object> fetchMap, List<String> list, String tableName,String relateKey,StringBuilder sb) {
		fetchMap.remove("sb");
		List<String> ids = list;
		sb.append("delete from ").append(tableName).append(" where ").append(relateKey).append(" in (");
		if (CollectionUtils.isEmpty(ids))
			ids = (List<String>) fetchMap.get(relateKey);
		for (String str : ids)
			sb.append("'").append(str).append("',");
		sb.deleteCharAt(sb.length() - 1);
		sb.append(")");
		ids = null;
		return sb;
	}

	@Override
	protected String fileDir(String schema, String table,String month) {
		return "/tmp/mysqldata_test_delete/" + schema + "/" + table + "/" + month;
	}

	@Override
	protected String appName(String schema, String table) {
		return "export  mysqltable delete sql" + schema + ":" + table + SparkHiveUtil.now();
	}
	
	
	
}
