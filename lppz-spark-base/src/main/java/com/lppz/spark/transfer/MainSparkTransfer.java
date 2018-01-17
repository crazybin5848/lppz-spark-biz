package com.lppz.spark.transfer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.lppz.spark.bean.SparkHiveSqlBean;
import com.lppz.spark.bean.SparkMysqlDmlBean;
import com.lppz.spark.bean.SparkSqlConfigBean;
import com.lppz.spark.support.EnumTransfer;
import com.lppz.spark.support.SparkContainer;
import com.lppz.spark.support.SparkTransferFactory;
import com.lppz.spark.util.SparkYamlUtils;

public class MainSparkTransfer {

	public static void main(String[] args) throws IOException {
		args = new String[] { "./META-INF/multimysql2hive.yaml", "local[8]",
				"month,;2015-08,;maxdate,;'2015-09',;mindate,;'2015-08',;total4Once,;100,;span,;1" };
		if (args.length == 0)
			throw new IOException("need yaml config");
		String[] params = args[args.length - 1].split(",;");
		String stCommand = EnumTransfer.getType("delete");
		String month = null;
		String od = null;
		Long total4Once = null;
		for (int i = 0; i < params.length;) {
			if ("month".equals(params[i])) {
				month = params[i + 1];
				if (LoadSparkTransfer.class.getSimpleName().equals(stCommand) && !"'".equals(month.charAt(0))
						&& !"'".equals(month.charAt(month.length() - 1))) {
					od = args[args.length - 1].replaceFirst("month,;" + params[i + 1],
							"month,;" + "'" + params[i + 1] + "'");
				}
			}
			if ("total4Once".equals(params[i]))
				total4Once = Long.valueOf(params[i + 1]);
			i += 2;
		}
		if (od != null)
			args[args.length - 1] = od;
		if (month == null)
			throw new IOException("need month config");
		SparkHiveSqlBean bean = SparkYamlUtils.loadYaml(args[0], true);
		String mode = args.length == 1 ? "local" : args[1];
		if (total4Once != null && total4Once != 0)
			bean.getMysqlBean().setTotal4Once(total4Once);
		if (DeleteSparkTransfer.class.getSimpleName().equals(stCommand))
			buildMonthInterval(args);
		start(bean, mode, month, SparkTransferFactory.getSparkTransfer(ParseNewSparkTransfer.class.getSimpleName()), args);

		if (ParseNewSparkTransfer.class.getSimpleName().equals(stCommand))
			return;
		SparkTransfer sparkTransfer = SparkTransferFactory.getSparkTransfer(stCommand);
		if (sparkTransfer == null)
			throw new RuntimeException(" do not support the command:" + stCommand);
		start(bean, mode, month, sparkTransfer, args);
	}

	public static void start(SparkHiveSqlBean bean, String mode, String month, SparkTransfer st, String... args)
			throws IOException {

		Map<String, Integer> maxAndMin = st.fetchMaxAndMin(bean.getConfigBean(), bean.getMysqlBean());
		int max = maxAndMin == null || maxAndMin.get("max") == null ? 0 : maxAndMin.get("max");
		int min = maxAndMin == null || maxAndMin.get("min") == null ? 0 : maxAndMin.get("min");
		maxAndMin = null;
		SparkContainer sparkContainer = new SparkContainer();
		try {
			doExcute(bean, mode, month, st, min, max, sparkContainer, args);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sparkContainer.getSc() != null)
				sparkContainer.getSc().stop();
		}

	}

	public static void doExcute(SparkHiveSqlBean bean, String mode, String month, SparkTransfer st, long start,
			long max, SparkContainer sc, String... args) throws IOException {
		Map<String, SparkHiveSqlBean> relationalMap = bean.getSparkMapbean();
		Map<String, Object> map = new HashMap<String, Object>();
		long step = bean.getMysqlBean().getTotal4Once() == null ? 0 : bean.getMysqlBean().getTotal4Once();
		if (max != 0) {
			bean.getMysqlBean().setOffset(start);
			bean.getMysqlBean().setTotal4Once(start + step);
		}
		try {
			st.excute(bean, bean.getConfigBean(), mode, sc, map, month, null, bean.getMysqlBean(), null, args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		Map<String, Object> nextMap = new HashMap<String, Object>();
		if (relationalMap != null && relationalMap.size() != 0)
			circulateSparkTransfer(relationalMap, mode, bean.getConfigBean(), st, sc, map, month, bean.getMysqlBean(),
					nextMap, args);
		start += step;
		if (start > max) {
			return;
		}
		
		map = null;
		nextMap = null;
		doExcute(bean, mode, month, st, start, max, sc);
	}

	private static void circulateSparkTransfer(Map<String, SparkHiveSqlBean> sparkMapbean, String mode,
			SparkSqlConfigBean configBean, SparkTransfer st, SparkContainer sc, Map<String, Object> map, String month,
			SparkMysqlDmlBean dmlBean, Map<String, Object> nextMap, String... args) {

		for (Entry<String, SparkHiveSqlBean> entry : sparkMapbean.entrySet()) {
			try {
				if (entry.getValue().isMysqsqlUseMain())
					entry.getValue().setConfigBean(configBean);
				st.excute(entry.getValue(), configBean, mode, sc, map, month, entry.getKey(), dmlBean, nextMap, args);
				if (entry.getValue().getSparkMapbean() != null && entry.getValue().getSparkMapbean().size() != 0)
					circulateSparkTransfer(entry.getValue().getSparkMapbean(), mode, entry.getValue().getConfigBean(),
							st, sc, map, month, dmlBean, nextMap, args);
				nextMap.clear();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void buildMonthInterval(String[] args) {
		String[] params = args[args.length - 1].split(",;");
		Integer span = null;
		String currentDate = null;
		String mindate = null;
		String tempArgs = args[args.length - 1];
		for (int i = 0; i < params.length; i += 2) {
			if (params[i].equals("span")) {
				span = Integer.valueOf(params[i + 1]);
				break;
			}

		}
		if (span == null)
			return;
		if (span == 0) {
			tempArgs = tempArgs.replaceFirst(",;span,;" + span, "");
			args[args.length - 1] = tempArgs;
			return;
		}
		for (int i = 0; i < params.length; i += 2) {
			if (params[i].equals("maxdate")) {
				currentDate = params[i + 1];
				currentDate = '\'' == currentDate.charAt(0) && '\'' == currentDate.charAt(currentDate.length() - 1)
						? currentDate.substring(1, currentDate.length() - 1) : currentDate;
			}
			if (params[i].equals("mindate"))
				mindate = params[i + 1];
		}
		try {
			Calendar calendar = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = sdf.parse(currentDate);
			calendar.setTime(date);
			calendar.set(calendar.MONTH, calendar.get(Calendar.MONTH) - span);
			tempArgs = tempArgs.replaceFirst("maxdate,;'" + currentDate + "'",
					"maxdate,;'" + sdf.format(calendar.getTime()) + "'");
			calendar.set(calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) - 1);
			if (mindate != null)
				tempArgs = tempArgs.replaceFirst("mindate,;" + mindate,
						"mindate,;'" + sdf.format(calendar.getTime()) + "'");
			else
				tempArgs = tempArgs + ",;mindate,;'" + sdf.format(calendar.getTime()) + "'";
			
			
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
		tempArgs = tempArgs.replaceFirst(",;span,;" + span, "");
		args[args.length - 1] = tempArgs;
	}

}
