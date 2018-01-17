package com.lppz.spark;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import com.lppz.spark.bean.Rdbms2HDfsBean;
import com.lppz.spark.bean.SparkHiveSqlBean;
import com.lppz.spark.bean.SparkMysqlDmlBean;
import com.lppz.spark.bean.SparkSqlConfigBean;
import com.lppz.spark.scala.HistoryOrderHandler;
import com.lppz.spark.scala.SparkHdfsUtil;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.support.TransferDate;
import com.lppz.spark.transfer.MainSparkTransfer;
import com.lppz.spark.transfer.ParseNewSparkTransfer;
import com.lppz.spark.util.SparkHiveUtil;
import com.lppz.spark.util.SparkYamlUtils;

public class ExportAllOrderDataSpark {
	private static Logger log = Logger.getLogger(ExportAllOrderDataSpark.class);
	static boolean isTest=false;
	static boolean dataType=true;
	public static void main(String[] args) throws Exception {
//		args = new String[] { "F:\\o2ooms\\zwrf1.yaml", "local[128]", "month,;2015-08,;maxdate,;'2015-08-02',;mindate,;'2015-08-01',;span,;0,;total4Once,;50000" };
		SparkContext sc=null;
		SparkHiveSqlBean bean =null;
		ExportAllOrderDataSpark eaod=new ExportAllOrderDataSpark();
		try {
			if (args.length == 0)
				throw new IOException("need yaml config");
			String[] params = args[args.length - 1].split(",;");
			String month = null;
			Long total4Once = null;
			for(int i=0;i<params.length;){
				if("month".equals(params[i]))
					month = params[i+1];
				if ("total4Once".equals(params[i]))
					total4Once = Long.valueOf(params[i + 1]);
				if ("dataType".equals(params[i]))
					dataType = Boolean.valueOf(params[i + 1]);
				i+=2;
			}
			if(month ==null)
				throw new IOException("need month config");
			bean = SparkYamlUtils.loadYaml(args[0], false);
			if (total4Once != null && total4Once != 0)
				bean.getMysqlBean().setTotal4Once(total4Once);
			String mode = args.length == 1 ? "local" : args[1];
			MainSparkTransfer.buildMonthInterval(args);
			TransferDate tfd = new TransferDate().buildDate(params, month);
			if(tfd.getMaxdate()==null)
				throw new IOException("need maxdate");
			ParseNewSparkTransfer.parseAll(bean, args);
			HistoryOrderHandler mysql = new HistoryOrderHandler();
			String appName = "export all" + bean.getConfigBean().getSchema() + ":" + bean.getMysqlBean().getTableName() + SparkHiveUtil.now();
			sc = new MysqlSpark().buildSc(appName, mode);
			eaod.dofetchMysqlFromSparkToLocal(mysql,sc,bean.getConfigBean(), bean.getMysqlBean(), mode, bean.getSparkMapbean()==null?null:bean.getSparkMapbean(),
					month,tfd,bean.getSourcebean());
			log.info("fetch data end;");
		} catch (Exception e) {
			log.error(e.getMessage(),e);
			throw e;
		}
		finally{
			if(sc!=null)
			sc.stop();
			FileSystem fs=new SparkHdfsUtil().getFileSystem(bean.getSourcebean().getHdfsUrl());
			fs.delete(new Path("/tmp/mysqlhive"), true);
			fs.close();
		}
	}

	public String dofetchMysqlFromSparkToLocal(HistoryOrderHandler mysql, SparkContext sc,SparkSqlConfigBean config, SparkMysqlDmlBean bean, String mode,
			Map<String, SparkHiveSqlBean> map2, String month, TransferDate tfd, Rdbms2HDfsBean rdbms2hDfsBean) throws Exception {
		String fileDir = "/tmp/mysqldata/" + config.getSchema() + "/" + bean.getTableName().replaceAll("`", "") + "/" + month;
		if (bean.getTotal4Once() != null) {
			Long tt = bean.getTotal4Once();
			Map<String,Integer> maxAndMin = fetchMaxAndMin(config,bean);
			int max = maxAndMin.get("max");// maxIdList.get(0).getInt(0);
			int min =  maxAndMin.get("min");// maxIdList.get(0).getInt(0);
			for (long i = min; i <= max; i += tt+1) {
				bean.setOffset(i);
				bean.setTotal4Once(tt + i>max?max:tt + i);
				Map<String,String> map=mysql.getMysqlListAndSave2Hdfs(sc.appName(), mode,rdbms2hDfsBean.getHiveschema(),rdbms2hDfsBean.getHivetableName(),rdbms2hDfsBean.getHdfsUrl(),rdbms2hDfsBean.getHpcList().get(0).getValue(),config, bean, sc,tfd.getMindate(),dataType,isTest);
				if(map==null||map.size()==0)
					continue;
				String pgNum = bean.getOffset()+"_"+bean.getTotal4Once();
				StringBuilder strdel = buildDelData(bean.getTableName().replaceAll("`", ""),"id",null," between ".concat(String.valueOf(bean.getOffset())).concat(" and ").concat(String.valueOf(bean.getTotal4Once())),config.getSchema(),tfd.getMindate());
				if(dataType)
				mysql.saveDelSql2Hdfs(sc.appName(), mode, rdbms2hDfsBean.getHiveschema(), rdbms2hDfsBean.getHivetableName(), rdbms2hDfsBean.getHdfsUrl(), new String[]{strdel.toString()}, month, pgNum, sc,isTest);
				if(map2!=null && !map2.isEmpty()){
					fetchRelationalTables(config, bean, mode, mysql, sc, map, map2,  month,tfd);
				}
			}
		} 
		return fileDir;
	}

	private StringBuilder buildDelData(String tableName,String colName,String id,String cond, String schema,String date) {
		StringBuilder strdel = new StringBuilder();
		strdel.append(Long.toString(Math.abs(UUID.randomUUID().getMostSignificantBits()), 36)).append("\t").append(schema).append("\t").append(tableName).append("\t").append(colName==null?"NULL":colName).append("\t").append(id==null?"NULL":id).append("\t").append(cond==null?"NULL":cond).append("\t").append(date);
		return strdel;
	}

	private void fetchRelationalTables(SparkSqlConfigBean config, SparkMysqlDmlBean bean, String mode,
			HistoryOrderHandler mysql, SparkContext sc, Map<String, String> map, Map<String, SparkHiveSqlBean> relationalMap,
			 String month, TransferDate tfd) throws IOException {
		for (Entry<String, SparkHiveSqlBean> entry : relationalMap.entrySet()) {
			SparkHiveSqlBean crruntBean = entry.getValue();
			boolean again = entry.getValue().getSparkMapbean() != null&&entry.getValue().getSparkMapbean().size()!=0;
			SparkMysqlDmlBean crruntsparkBean = crruntBean.getMysqlBean();
			crruntsparkBean.setOffset(bean.getOffset());
			crruntsparkBean.setTotal4Once(bean.getTotal4Once());
			String currentAppName = "export all " + ( crruntBean.isMysqsqlUseMain() ? config.getSchema() :crruntBean.getConfigBean().getSchema()) + ":" + crruntsparkBean.getTableName() + SparkHiveUtil.now();
			String col = entry.getKey().split("_")[0];
			String id = null;
			for(String str:map.keySet()){
				if(str.startsWith(col.concat("_")))
					id = str.replaceFirst(col.concat("_"), "");
			}
			String pgNum = bean.getOffset()+"_"+bean.getTotal4Once();
			StringBuilder strdel = buildDelData(crruntBean.getMysqlBean().getTableName().replaceAll("`", ""),crruntBean.getMysqlBean().getRelateKey().replaceAll("`", ""),id,null,crruntBean.isMysqsqlUseMain() ? config.getSchema():crruntBean.getConfigBean().getSchema(),tfd.getMindate());
			if(dataType)
			mysql.saveDelSql2Hdfs(currentAppName, mode, crruntBean.getSourcebean().getHiveschema(), crruntBean.getSourcebean().getHivetableName(), crruntBean.getSourcebean().getHdfsUrl(), new String[]{strdel.toString()}, month, pgNum, sc,isTest);
			String oidList = id==null?null:map.get(col.concat("_").concat(id));
			if(StringUtils.isBlank(oidList))
				crruntsparkBean.setColArray(new String[]{"0=1"});
			else{
				List<String> list=Arrays.asList(oidList.split(","));
				crruntsparkBean.buildSplitArray(list);
			}
			String excuteSql = new StringBuilder(" select * from ").append(crruntsparkBean.getTableName()).toString();
			log.info(excuteSql);
			Map<String,String> tempMap=mysql.getMysqlListAndSave2Hdfs(currentAppName, mode,crruntBean.getSourcebean().getHiveschema(),crruntBean.getSourcebean().getHivetableName(),crruntBean.getSourcebean().getHdfsUrl(),crruntBean.getSourcebean().getHpcList().get(0).getValue(),crruntBean.isMysqsqlUseMain() ? config : crruntBean.getConfigBean(), crruntsparkBean, sc,tfd.getMindate(),dataType,isTest);
			if (again)
				fetchRelationalTables(crruntBean.isMysqsqlUseMain() ? config : crruntBean.getConfigBean(), bean, mode, mysql, sc, tempMap, crruntBean.getSparkMapbean(),
						 month,tfd);
		}

	}
	
	private Map<String,Integer> fetchMaxAndMin(SparkSqlConfigBean config, SparkMysqlDmlBean bean) throws Exception {
		Connection conn = null;
		String sql = bean.getSql().replaceFirst("\\*", "max(".concat(bean.getPartitionColumn()).concat("), min(".concat(bean.getPartitionColumn()).concat(") ")));
		String url = config.getRdbmsjdbcUrl();
		Map<String,Integer> map = new HashMap<String,Integer>(2);
		try {
			Class.forName(config.getRdbmsdbDriver());
			conn = DriverManager.getConnection(url, config
					.getRdbmsjdbcUser(), config.getRdbmsjdbcPasswd());
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while(rs.next()) {
				map.put("max", rs.getInt(1));
				map.put("min", rs.getInt(2));
			}			
			return map;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
			}
		}
	}
}