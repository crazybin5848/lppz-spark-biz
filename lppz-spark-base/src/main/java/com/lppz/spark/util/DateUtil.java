package com.lppz.spark.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtil {
	private static Logger log = LoggerFactory.getLogger(DateUtil.class);
	public static boolean month(String month) {
		Pattern p = Pattern.compile("^([2][0]\\d{2})-(0[1-9]|1[0-2])$");
		Matcher m = p.matcher(month);
		return m.matches();
	}
	
	public static boolean day(String day) {
		Pattern p = Pattern.compile("^([2][0]\\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|[3][0-1])$");
		Matcher m = p.matcher(day);
		return m.matches();
	}
	
	public static boolean hour(String hour) {
		Pattern p = Pattern.compile("^([2][0]\\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|[3][0-1])\\s{1,}([0-1][0-9]|[2][0-3])$");
		Matcher m = p.matcher(hour);
		return m.matches();
	}
	
	public static boolean min(String min) {
		Pattern p = Pattern.compile("^([2][0]\\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|[3][0-1])\\s{1,}([0-1][0-9]|[2][0-3]):([0-5]\\d|60)$");
		Matcher m = p.matcher(min);
		return m.matches();
	}
	
	public static boolean ss(String min) {
		Pattern p = Pattern.compile("^([2][0]\\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|[3][0-1])\\s{1,}([0-1][0-9]|[2][0-3]):([0-5]\\d|60):([0-5]\\d|60)$");
		Matcher m = p.matcher(min);
		return m.matches();
	}
	
	public static String parse(String date){
//		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat sdf =null;
		if(month(date)){
			sdf = new SimpleDateFormat("yyyy-MM");
		}
		else if(day(date)){
			sdf = new SimpleDateFormat("yyyy-MM-dd");
		}
		else if(hour(date)){
			sdf = new SimpleDateFormat("yyyy-MM-dd HH");
		}
		else if(min(date)){
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		}else{
			log.info("con't parse date:"+date);
			return date;
		}
		Date d = null;
		try {
			d = sdf.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
//		calendar.setTime(d);
//		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(d);
	}
	
	
}
