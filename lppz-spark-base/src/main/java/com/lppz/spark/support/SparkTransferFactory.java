package com.lppz.spark.support;

import java.util.HashMap;
import java.util.Map;

import com.lppz.spark.transfer.DeleteSparkTransfer;
import com.lppz.spark.transfer.GenerateSparkTransfer;
import com.lppz.spark.transfer.LoadSparkTransfer;
import com.lppz.spark.transfer.OrderSparkTransfer;
import com.lppz.spark.transfer.ParseSparkTransfer;
import com.lppz.spark.transfer.SparkTransfer;

public class SparkTransferFactory {
	public static Map<String, SparkTransfer> sts = new HashMap<String, SparkTransfer>();
	static {
		sts.put(DeleteSparkTransfer.class.getSimpleName(), new DeleteSparkTransfer());
		sts.put(OrderSparkTransfer.class.getSimpleName(), new OrderSparkTransfer());
		sts.put(ParseSparkTransfer.class.getSimpleName(), new ParseSparkTransfer());
		sts.put(LoadSparkTransfer.class.getSimpleName(), new LoadSparkTransfer());
		sts.put(GenerateSparkTransfer.class.getSimpleName(), new GenerateSparkTransfer());
	}

	public static SparkTransfer getSparkTransfer(String transferName) {
		return sts.get(transferName);
	}
}
