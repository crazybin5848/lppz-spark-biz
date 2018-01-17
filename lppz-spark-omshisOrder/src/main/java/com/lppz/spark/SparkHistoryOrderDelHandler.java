/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lppz.spark;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.springframework.core.io.FileSystemResource;
import org.yaml.snakeyaml.Yaml;

import com.lppz.core.datasource.DynamicDataSource;
import com.lppz.core.datasource.MycatClusterDynamicDataSource;
import com.lppz.spark.scala.HistoryOrderDelHandler;
import com.lppz.spark.scala.HiveContextUtil;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.support.TransferDate;

public class SparkHistoryOrderDelHandler {
	private static final Logger LOG = Logger
			.getLogger(SparkHistoryOrderDelHandler.class);
	private static DynamicDataSource ds=null;

	public static void main(String[] args) throws IOException {
		SparkContext sc=null;
		String month=null;
		String mode=null;
//		args=new String[]{"mode,;local[8],;month,;2015-07,;maxdate,;'2015-08-01',;mindate,;'2015-07-31',;dsPath,;/Users/zoubin/IdeaProjects/MixedScalaProj/Spark-History-Order/src/main/resources/META-INF/ds-multi.yaml"};
		try {
			if (args.length == 0)
				throw new IOException("need yaml config");
			String[] params = args[0].split(",;");
			
			LOG.info("args[0]:"+args[0]);
			
			String dsPath=null;
			for(int i=0;i<params.length;){
				if("mode".equals(params[i]))
					mode=params[i+1];
				if("month".equals(params[i]))
					month = params[i+1];
				if("dsPath".equals(params[i]))
					dsPath = params[i+1];
				i+=2;
			}
			TransferDate tfd = new TransferDate().buildDate(params, month);
			MysqlSpark mysql = new MysqlSpark();
			String appName = "Exec all delete mysql sql";
			sc = mysql.buildSc(appName, mode);
			SparkHistoryOrderDelHandler shod=new SparkHistoryOrderDelHandler();
			shod.initDs(dsPath);
			
			if(null==SparkHistoryOrderDelHandler.ds){
				LOG.error("dsPath not found");
				System.exit(-1);
			}
			
//			hc.exec(sc, "drop schema omsextdel cascade");
//			hc.exec(sc, "create schema omsextdel");
//			hc.exec(sc, "use omsextdel");
//			hc.exec(sc, "create table omsdel(id varchar(30),schemaName varchar(30),tableName varchar(50),colName varchar(30),condPk varchar(30),whereCond varchar(255),createTime timestamp) PARTITIONED BY (ds string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' STORED AS TEXTFILE");
//			hc.exec(sc, "create table omsdeldetail(id varchar(30),colInStr string,createTime timestamp,pgNum varchar(30)) PARTITIONED BY (ds string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' STORED AS TEXTFILE");
//			String sql1="load data local inpath '/tmp/omsdel/ds=2015-07' overwrite into table omsdel PARTITION (ds='2015-07')";
//			hc.exec(sc, sql1);
//			sql1="load data local inpath '/tmp/omsdeldetail/ds=2015-07' overwrite into table omsdeldetail PARTITION (ds='2015-07')";
//			hc.exec(sc, sql1);
			HistoryOrderDelHandler hoh=new HistoryOrderDelHandler();
			HiveContextUtil.exec(sc, "use omsextdel");
			String sqlStr="select a.schemaname as schema,a.tablename as tbl,a.colname as col,a.condpk as pk,a.wherecond as cond,b.colinstr as colin from omsdel a left join omsdeldetail b on a.condpk=b.id where b.createtime>='"+tfd.getMindate()+"' and b.createtime<'"+tfd.getMaxdate()+"' and a.ds='"+month+"' and b.ds='"+month+"'";
			LOG.info("SparkDelsql2:"+sqlStr);
			hoh.execQueryAndExecDelSql(sc, sqlStr, ds);
			sqlStr="select schemaname as schema,tablename as tbl,colname as col,condpk as pk,wherecond as cond from omsdel where tablename='order_sharding' and ds='"+month+"' and createtime<'"+tfd.getMaxdate()+"' and createtime>='"+tfd.getMindate()+"'";
			LOG.info("SparkDelsql1:"+sqlStr);
			hoh.execQueryAndExecDelSql(sc, sqlStr, ds);
		
		}catch(Exception ex){
			LOG.error(ex.getMessage(),ex);
		}
		finally{
			if(sc!=null)
			sc.stop();
			if(ds!=null)
				try {
					ds.destory();
				} catch (IllegalArgumentException | IllegalAccessException e) {
					LOG.error(e.getMessage(),e);
				}
		}
	}
	
	public void initDs(String dsYamlConfPath) {
		try {
			if (StringUtils.isNotBlank(dsYamlConfPath)) {
				if (new File(dsYamlConfPath).exists()) {
					ds = (MycatClusterDynamicDataSource) new Yaml()
					.load(new FileSystemResource(dsYamlConfPath).getInputStream());
				} 
			}
			if(null!=ds){
				ds.afterPropertiesSet();
			}
		} catch (IOException e) {
			LOG.error(e.getMessage(),e);
		}
	}

	public static DynamicDataSource getDs() {
		return ds;
	}

	public static void setDs(DynamicDataSource ds) {
		SparkHistoryOrderDelHandler.ds = ds;
	}
}