package com.lppz.spark.transfer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;

import com.lppz.spark.bean.HivePartionCol;
import com.lppz.spark.bean.SparkHiveSqlBean;
import com.lppz.spark.util.SparkYamlUtils;

public class ParseNewSparkTransfer {

	public static void main(String[] args)throws IOException  {
//		args = new String[] { "./META-INF/multimysql2hive.yaml", "local[8]", "month,;2016-09,;maxdate,;'2016-09',;mindate,;'2016-07'"};
		if (args.length == 0)
			throw new IOException("need yaml config");
		SparkHiveSqlBean bean = SparkYamlUtils.loadYaml(args[0], false);
		String mode = args.length == 1 ? "local" : args[1];
		parseAll(bean, args);
		System.out.println();
	}
	
	public static void parseAll(SparkHiveSqlBean shsb, String[] args){
		parse(shsb, args);
		if(null!=shsb.getSparkMapbean() && !shsb.getSparkMapbean().isEmpty())
			circulateParseHpcList(shsb.getSparkMapbean(), args);
	}
	
	private static void circulateParseHpcList(Map<String, SparkHiveSqlBean> map, String[] args){
		for (Entry<String, SparkHiveSqlBean> entry : map.entrySet()){
//			parseHpcList(entry.getValue(), args);
			parse(entry.getValue(), args);
			if(entry.getValue().getSparkMapbean()!=null&&entry.getValue().getSparkMapbean().size()!=0)
				circulateParseHpcList(entry.getValue().getSparkMapbean(), args);
		}
	}
	
	private static void buildMonthInterval(String[] args){
		String[] params = args[args.length - 1].split(",;");
		String month = null;
		for (int i = 0; i < params.length; i += 2) {
			if(params[i].equals("issuedate")){
				month = params[i+1];
				break;
			}
		}
		try {
			 Calendar calendar=Calendar.getInstance();  
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
            Date date = sdf.parse(month);
            calendar.setTime(date);
            calendar.set(calendar.MONTH, calendar.get(Calendar.MONTH)+1);
            System.out.println(sdf.format( calendar.getTime()));
            calendar.setTime(date);
            calendar.set(calendar.MONTH, calendar.get(Calendar.MONTH)-1);
            System.out.println(sdf.format( calendar.getTime()));
        } catch (ParseException e) {
            e.printStackTrace();
        }
		
	}
	
	public static void parse(SparkHiveSqlBean shsb, String[] args){
		if(shsb.getConfigBean()!=null&&shsb.getConfigBean().getRdbmsjdbcUrl()!=null){
			String url=shsb.getConfigBean().getRdbmsjdbcUrl().replaceAll("#schema#", shsb.getConfigBean().getSchema());
			shsb.getConfigBean().setRdbmsjdbcUrl(url);
		}
		parseHpcList(shsb, args);
		if(shsb.getMysqlBean()!=null){
		String sql = shsb.getMysqlBean().getSql();
		if (sql!=null&&sql.contains("#")) {
			shsb.getMysqlBean().setSql(replaceAll(args, sql));
		}
		}
	}


	private static void parseHpcList(SparkHiveSqlBean shsb, String[] args) {
		if (CollectionUtils.isNotEmpty(shsb.getSourcebean().getHpcList())) {
			String[] ss = args[args.length - 1].split(",;");
			int k = 0;
			for (HivePartionCol hpc : shsb.getSourcebean().getHpcList()) {
				String val = hpc.getValue().replaceAll("#" + ss[k] + "#",
						ss[k + 1]);
				k += 2;
				hpc.setValue(val);
			}
		}
	}

	
	private static String replaceAll(String[] args,String str){
		String[] parmas = args[args.length - 1].split(",;");
		String result = str;
		for (int i = 0; i < parmas.length; i += 2) {
			result = result.replaceAll("#" + parmas[i] + "#", parmas[i + 1]);
		}
		return result;
	}
}
