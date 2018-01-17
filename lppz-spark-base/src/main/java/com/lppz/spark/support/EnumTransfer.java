package com.lppz.spark.support;

import com.lppz.spark.transfer.DeleteSparkTransfer;
import com.lppz.spark.transfer.GenerateSparkTransfer;
import com.lppz.spark.transfer.LoadSparkTransfer;
import com.lppz.spark.transfer.OrderSparkTransfer;
import com.lppz.spark.transfer.ParseSparkTransfer;

public enum EnumTransfer {
	parse(ParseSparkTransfer.class.getSimpleName()),output(OrderSparkTransfer.class.getSimpleName()),delete(DeleteSparkTransfer.class.getSimpleName()),load(LoadSparkTransfer.class.getSimpleName()),create(GenerateSparkTransfer.class.getSimpleName());
	
	private String type;
	
	private EnumTransfer(String type){
		this.type = type;
	}
	
	public static String getType(String name){
		return EnumTransfer.valueOf(name).type;
	}
	
}
