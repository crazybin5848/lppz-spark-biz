package com.lppz.spark.support;

import com.lppz.spark.util.DateUtil;

public class TransferDate {

	private String month;
	private String maxdate;
	private String mindate;
	private long currentTime;
	
	public TransferDate(){}
	public TransferDate(String month, String maxdate, String mindate) {
		super();
		this.month = month;
		this.maxdate = maxdate;
		this.mindate = mindate;
		this.currentTime = System.currentTimeMillis();
	}

	public String getMonth() {
		return month;
	}
	
	public String getMaxdate() {
		return maxdate;
	}
	
	public String getMindate() {
		return mindate;
	}
	public void setMindate(String mindate) {
		this.mindate = mindate;
	}
	public long getCurrentTime() {
		return currentTime;
	}
	
	public TransferDate buildDate(String[] params, String month) {
		String maxdate = null;
		String mindate = null;
		for(int i=0;i<params.length;){
			if("maxdate".equals(params[i]))
				maxdate = params[i+1];
			if ("mindate".equals(params[i]))
				mindate = params[i+1];
			i+=2;
		}
		if(mindate!=null)
			mindate = mindate.replaceAll("'", "");
		if(maxdate!=null)
			maxdate = maxdate.replaceAll("'", "");
		this.month=month;
		this.maxdate=DateUtil.parse(maxdate);
		this.mindate=DateUtil.parse(mindate);
		return this;
	}
}