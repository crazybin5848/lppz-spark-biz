package com.lppz.spark.transfer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;

import com.lppz.spark.bean.SparkHiveSqlBean;
import com.lppz.spark.bean.SparkMysqlDmlBean;
import com.lppz.spark.bean.SparkSqlConfigBean;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.support.SparkContainer;
import com.lppz.spark.util.FileNioUtil;

public abstract class FetchSparkTransfer implements SparkTransfer {
	@SuppressWarnings("unchecked")
	@Override
	public void excute(SparkHiveSqlBean bean, SparkSqlConfigBean config, String mode, SparkContainer sparkContainer,
			Map<String, Object> map, String month, String key, SparkMysqlDmlBean dmlBean, Map<String, Object> nextMap, String... args)
			throws IOException {
//		System.out.println("/tmp/mysqldata_test_delete/" + bean.getConfigBean().getSchema() + "/"
//				+ bean.getMysqlBean().getTableName());
		String fileDir = fileDir(config.getSchema(), bean.getMysqlBean().getTableName(), month);
		String appName = appName(config.getSchema(), bean.getMysqlBean().getTableName());
		String relateKey = null;
		if (sparkContainer.getMysql() == null) {
			MysqlSpark mysql = new MysqlSpark();
			SparkContext sparkContext = mysql.buildSc(appName, mode);
			sparkContainer.setMysql(mysql);
			sparkContainer.setSc(sparkContext);
		}
		List<Row> list = null;
		StringBuilder sb = null;
		Map<String, Object> fetchMap = null;
		List<String> oidList = null;
		if (key == null) {
			list = sparkContainer.getMysql().getMysqlList(appName, mode, config, bean.getMysqlBean(),
					sparkContainer.getSc());
			relateKey = "id";
		} else {
			
			oidList = nextMap != null && nextMap.size() != 0 ? (List<String>) nextMap.get(key.split("_")[0])
					: (List<String>) map.get(key.split("_")[0]);
			SparkMysqlDmlBean crruntsparkBean = bean.getMysqlBean();
			relateKey = crruntsparkBean.getRelateKey();
			crruntsparkBean.buildSplitArray(oidList);
			String excuteSql = new StringBuilder(" select * from ").append(crruntsparkBean.getTableName())
					.append(" size: ").append(oidList.size()).toString();
			System.out.println(excuteSql);
			list = sparkContainer.getMysql().getMysqlListArray(appName, mode, bean.getConfigBean(), crruntsparkBean,
					sparkContainer.getSc());
		}
		if (CollectionUtils.isEmpty(list))
			return;
		fetchMap = buildString(list, bean.getSparkMapbean() == null || bean.getSparkMapbean().size() == 0
				? new String[0] : bean.getMysqlBean().getColList().split(","));
		list = null;
		sb = new StringBuilder();
		generateOutput(fetchMap, oidList, bean.getMysqlBean().getTableName(), relateKey,sb);
		fetchMap.remove("sb");
		oidList = null;
		FileNioUtil.writeWithMappedByteBuffer(fileDir, dmlBean.getOffset() + "_" + dmlBean.getTotal4Once(), sb);
		sb = null;
		if (map.size() == 0)
			map.putAll(fetchMap);
		if (nextMap != null) {
			nextMap.clear();
			nextMap.putAll(fetchMap);
		}
		fetchMap = null;
	}

	protected abstract StringBuilder generateOutput(Map<String, Object> fetchMap, List<String> list, String tableName,
			String relateKey,StringBuilder sb);

	protected abstract String fileDir(String schema, String table, String month);

	protected abstract String appName(String schema, String table);

	protected Map<String, Object> buildString(List<Row> list, String... col) {
		Map<String, Object> map = new HashMap<String, Object>(col == null ? 1 : col.length + 1);
		StringBuilder sb = new StringBuilder("");
		Map<String, List<String>> colMap = new HashMap<String, List<String>>(col == null ? 1 : col.length);

		int[] colIdxs = new int[col == null ? 0 : col.length];
		for (int i = 0; i < colIdxs.length; i++) {
			colIdxs[i] = list.get(0).fieldIndex(col[i]);
			List<String> listOid = new ArrayList<String>();
			colMap.put(col[i], listOid);
		}
		for (Iterator<Row> i$ = list.iterator(); i$.hasNext(); sb.append("\n")) {
			Row r = (Row) i$.next();
			for (int i = 0; i < r.length(); i++) {
				Object o = r.get(i);
				if (o instanceof String) {
					o = ((String) o).replaceAll("\n", " ");
					o = ((String) o).replaceAll("\t", " ");
				}
				sb.append(o);
				if (i < r.length() - 1)
					sb.append("\t");
				int colIndex = arrayContain(colIdxs, i);
				if (colIndex != -1)
					colMap.get(col[colIndex]).add((String) o);
			}
		}
		map.put("sb", sb);
		map.putAll(colMap);
		return map;
	}

	@Override
	public Map<String, Integer> fetchMaxAndMin(SparkSqlConfigBean config, SparkMysqlDmlBean bean) {
		Connection conn = null;

		String sql = bean.getSql().replaceFirst("\\*", "max(".concat(bean.getPartitionColumn())
				.concat("), min(".concat(bean.getPartitionColumn()).concat(") ")));
		String url = config.getRdbmsjdbcUrl();
		Map<String, Integer> map = new HashMap<String, Integer>(2);
		try {
			Class.forName(config.getRdbmsdbDriver());
			conn = DriverManager.getConnection(url, config.getRdbmsjdbcUser(), config.getRdbmsjdbcPasswd());
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				map.put("max", rs.getInt(1));
				map.put("min", rs.getInt(2));
			}
			return map;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
			}
		}
		return null;
	}

	protected int arrayContain(int[] array, int arg) {
		int j = 0;
		for (int i : array) {
			if (arg == i)
				return j;
			j++;
		}
		return -1;
	}
}
