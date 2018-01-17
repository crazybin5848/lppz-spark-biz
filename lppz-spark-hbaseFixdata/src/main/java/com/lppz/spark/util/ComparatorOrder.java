package com.lppz.spark.util;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import com.lppz.oms.api.model.hbase.HbaseOrderBean;

public class ComparatorOrder implements Comparator<HbaseOrderBean>{

	@Override
	public int compare(HbaseOrderBean o1, HbaseOrderBean o2) {
		if (o1.getTimestamp() < o2.getTimestamp()) {
			return -1;
		}else if (o1.getTimestamp() > o2.getTimestamp()){
			return 1;
		}else {
			return 0;
		}
	}
	
	public static void main(String[] args) {
		Set<HbaseOrderBean> set = new TreeSet<HbaseOrderBean>(new ComparatorOrder());
		set.add(new HbaseOrderBean("hi1", 6l));
		set.add(new HbaseOrderBean("hi2", 1l));
		set.add(new HbaseOrderBean("hi3", 3l));
		set.add(new HbaseOrderBean("hi4", 9l));
		set.add(new HbaseOrderBean("hi5", 3l));
		for(HbaseOrderBean hoBean : set) {
			System.out.print(String.format("%s-%d, ", hoBean.getRowKey(), hoBean.getTimestamp()));
		}
		
	}
	
}