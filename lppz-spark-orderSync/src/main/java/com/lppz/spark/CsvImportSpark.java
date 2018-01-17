package com.lppz.spark;

import org.apache.spark.SparkContext;

import com.lppz.spark.scala.CsvHandler;
import com.lppz.spark.scala.jdbc.MysqlSpark;

public class CsvImportSpark {

	public static void main(String[] args) {
		SparkContext sc=null;
		
		sc = new MysqlSpark().buildSc("test csv spark", "local[8]");
		
		CsvHandler handler=new CsvHandler();
		
		handler.importCsv2Spark(sc);
	}

}
