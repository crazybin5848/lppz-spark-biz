package com.lppz.spark.sap.handler;

import java.io.Serializable;

import org.springframework.jdbc.core.JdbcTemplate;

import com.lppz.spark.rdbms.JdbcHandler;

public class HaNaJdbcHandler extends JdbcHandler implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3780396187591744829L;
	private String[] sqls;
	
	public HaNaJdbcHandler(){}

	public HaNaJdbcHandler(String[] sqls){
		this.sqls=sqls;
	}

	@Override
	public void handleInTrans(JdbcTemplate jt){
		jt.batchUpdate(sqls);
	}

	public String[] getSqls() {
		return sqls;
	}

	public void setSqls(String[] sqls) {
		this.sqls = sqls;
	}
}
