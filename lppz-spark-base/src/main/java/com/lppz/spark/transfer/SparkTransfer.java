package com.lppz.spark.transfer;

import java.io.IOException;
import java.util.Map;

import com.lppz.spark.bean.SparkHiveSqlBean;
import com.lppz.spark.bean.SparkMysqlDmlBean;
import com.lppz.spark.bean.SparkSqlConfigBean;
import com.lppz.spark.support.SparkContainer;

public interface SparkTransfer {

	public void excute(SparkHiveSqlBean bean, SparkSqlConfigBean config, String mode, SparkContainer sc,
			Map<String, Object> map, String month,String key,SparkMysqlDmlBean dmlBean,Map<String, Object> nextMap, String... args)throws IOException;
	public Map<String, Integer> fetchMaxAndMin(SparkSqlConfigBean config, SparkMysqlDmlBean bean);
}
