package com.lppz.spark.sap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import com.lppz.spark.sap.scala.ExportHanaIncreDataHandler;
import com.lppz.spark.sap.scala.Hive2RdbmsHandler;
import com.lppz.spark.sap.bean.Hive2DbmsBean;
import com.lppz.spark.scala.jdbc.MysqlSpark;
import com.lppz.spark.util.SparkHiveUtil;
import com.lppz.spark.util.SparkYamlUtils;

/***
 * 导入HANA增量数据
 * 
 * @author:xiaoyongfei
 */

public class ImportHANADataSpark {

	private static Logger log = Logger.getLogger(ImportHANADataSpark.class);

	static boolean isTest = false;

	public static void main(String[] args) throws Exception {
		// args = new String[] { "/home/hadoop/azihyd0800.yaml",
		// "local[8]","false",500000};

		if (args.length == 0)
			throw new IOException("need yaml config");

		isTest = Boolean.valueOf(args[2]);
		
		
		SparkContext sc = null;
		Hive2DbmsBean bean =null;
		
		String appName = "import hana table " + ":"+ SparkHiveUtil.now();
		sc = new MysqlSpark().buildSc(appName, args[1]);

		bean = SparkYamlUtils.loadYaml(args[0], isTest, Hive2DbmsBean.class);
		Hive2RdbmsHandler handler = new Hive2RdbmsHandler();
		handler.genCsvForHaNaWithHivePath(appName, bean, sc);
		
	}
	

}
