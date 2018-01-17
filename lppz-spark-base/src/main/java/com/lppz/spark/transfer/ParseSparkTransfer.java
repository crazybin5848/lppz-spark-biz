package com.lppz.spark.transfer;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.lppz.spark.bean.HivePartionCol;
import com.lppz.spark.bean.SparkHiveSqlBean;
import com.lppz.spark.bean.SparkMysqlDmlBean;
import com.lppz.spark.bean.SparkSqlConfigBean;
import com.lppz.spark.support.SparkContainer;

public class ParseSparkTransfer implements SparkTransfer {

	@Override
	public void excute(SparkHiveSqlBean bean, SparkSqlConfigBean config, String mode, SparkContainer sc,
			Map<String, Object> map, String month, String key, SparkMysqlDmlBean dmlBean, Map<String, Object> nextMap,
			String... args) throws IOException {
		if (bean.getConfigBean() != null && bean.getConfigBean().getRdbmsjdbcUrl() != null) {
			String url = bean.getConfigBean().getRdbmsjdbcUrl().replaceAll("#schema#",
					bean.getConfigBean().getSchema());
			bean.getConfigBean().setRdbmsjdbcUrl(url);
		}
		parseHpcList(bean, args);
		if (bean.getMysqlBean() != null) {
			String sql = bean.getMysqlBean().getSql();
			if (sql != null && sql.contains("#")) {
				bean.getMysqlBean().setSql(replaceAll(args, sql));
			}
		}
	}

	@Override
	public Map<String, Integer> fetchMaxAndMin(SparkSqlConfigBean config, SparkMysqlDmlBean bean) {
		return null;
	}

	private void parseHpcList(SparkHiveSqlBean shsb, String[] args) {
		if (CollectionUtils.isNotEmpty(shsb.getSourcebean().getHpcList())) {
			String[] ss = args[args.length - 1].split(",;");
			int k = 0;
			for (HivePartionCol hpc : shsb.getSourcebean().getHpcList()) {
				String val = hpc.getValue().replaceAll("#" + ss[k] + "#", ss[k + 1]);
				k += 2;
				hpc.setValue(val);
			}
		}
	}

	private String replaceAll(String[] args, String str) {
		String[] parmas = args[args.length - 1].split(",;");
		String result = str;
		for (int i = 0; i < parmas.length; i += 2) {
			result = result.replaceAll("#" + parmas[i] + "#", parmas[i + 1]);
		}
		return result;
	}

}
