package com.lppz.spark.accmember.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class LoadSqlBean implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1421482944916541986L;
	
	
	private Map<String,TableBean> tables;
	private Map<String,MyCatInfoBean> myCatInfo;
	private String jdbcDriver;


	public String getJdbcDriver() {
		return jdbcDriver;
	}


	public void setJdbcDriver(String jdbcDriver) {
		this.jdbcDriver = jdbcDriver;
	}


	public Map<String, MyCatInfoBean> getMyCatInfo() {
		return myCatInfo;
	}


	public void setMyCatInfo(Map<String, MyCatInfoBean> myCatInfo) {
		this.myCatInfo = myCatInfo;
	}


	public Map<String, TableBean> getTables() {
		return tables;
	}


	public void setTables(Map<String, TableBean> tables) {
		this.tables = tables;
	}


	public static void main(String[] args) {
		LoadSqlBean bean=new LoadSqlBean();
		
		bean.setMyCatInfo(new HashMap<String,MyCatInfoBean>());
		bean.setTables(new HashMap<String,TableBean>());
		bean.setJdbcDriver("com.mysql.jdbc.Driver");
		
		MyCatInfoBean _204=new MyCatInfoBean();
		_204.setTargetJdbcUrl("jdbc:mysql://10.8.202.204:7066/MEMBERDB?useConfigs=maxPerformance&characterEncoding=utf8");
		_204.setTargetPwd("root");
		_204.setTargetUser("root");
		
		MyCatInfoBean _205=new MyCatInfoBean();
		_205.setTargetJdbcUrl("jdbc:mysql://10.8.202.205:7066/MEMBERDB?useConfigs=maxPerformance&characterEncoding=utf8");
		_205.setTargetPwd("root");
		_205.setTargetUser("root");
		
		MyCatInfoBean _211=new MyCatInfoBean();
		_211.setTargetJdbcUrl("jdbc:mysql://10.8.202.211:7066/MEMBERDB?useConfigs=maxPerformance&characterEncoding=utf8");
		_211.setTargetPwd("root");
		_211.setTargetUser("root");
		
		MyCatInfoBean _212=new MyCatInfoBean();
		_212.setTargetJdbcUrl("jdbc:mysql://10.8.202.212:7066/MEMBERDB?useConfigs=maxPerformance&characterEncoding=utf8");
		_212.setTargetPwd("root");
		_212.setTargetUser("root");
		
		bean.getMyCatInfo().put("204", _204);
		bean.getMyCatInfo().put("205", _205);
		bean.getMyCatInfo().put("211", _211);
		bean.getMyCatInfo().put("212", _212);
		
		///////////////////////////////////////////
		TableBean member=new TableBean();
		member.setSourceTableOrView("lp_member");
		member.setTargetColumns(new ArrayList<String>());
		member.getTargetColumns().add("partner_no,card_no,member_no,telphone,source_type,member_account,member_pwd,password_encoding,birthday,create_time,create_user,edit_time,edit_user,register_store,recommender,member_status,sync_crm_status,sync_crm_time,member_type,member_level");
		bean.getTables().put("lp_member", member);
		
		TableBean lp_member_extend=new TableBean();
		lp_member_extend.setSourceTableOrView("lp_member_extend");
		lp_member_extend.setTargetColumns(new ArrayList<String>());
		lp_member_extend.getTargetColumns().add("id,member_no,partner_no,member_name,member_nickname,gender,educationcd,occupation,marital_status,income,id_type,id_number,header_imgpath,create_time,delete_flag,register_ip,register_terminal,terminal_type,email");
		bean.getTables().put("lp_member_extend", lp_member_extend);
		
		TableBean lp_member_address=new TableBean();
		lp_member_address.setSourceTableOrView("lp_member_address");
		lp_member_address.setTargetColumns(new ArrayList<String>());
		lp_member_address.getTargetColumns().add("id,member_no,partner_no,consignee,gender,position,longitude,dimensionality,map_type,address,cell_phone,province_code,province_name,city_code,city_name,district_code,district_name,create_time,edit_time,is_default,post_code");
		bean.getTables().put("lp_member_address", lp_member_address);
		
		TableBean lp_member_capital_account=new TableBean();
		lp_member_capital_account.setSourceTableOrView("lp_member_capital_account");
		lp_member_capital_account.setTargetColumns(new ArrayList<String>());
		lp_member_capital_account.getTargetColumns().add("id,partner_no,member_no,card_no,level_type,all_scores,credit_scores,behavior_scores,all_card_balance,card_balance,donation_amount,card_status");
		bean.getTables().put("lp_member_capital_account", lp_member_capital_account);
		
		TableBean lp_member_rela_accounts=new TableBean();
		lp_member_rela_accounts.setSourceTableOrView("lp_member_rela_accounts");
		lp_member_rela_accounts.setTargetColumns(new ArrayList<String>());
		lp_member_rela_accounts.getTargetColumns().add("id,member_no,partner_no,chanel_code,outer_user_id,outer_unionid,create_time,delete_flag");
		bean.getTables().put("lp_member_rela_accounts", lp_member_rela_accounts);
		
		Yaml y=new Yaml();
		
		System.out.println(y.dump(bean));
	}

}
