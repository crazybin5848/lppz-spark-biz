package com.lppz.spark.accmember.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class AccExportBean implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7749833770119273743L;

	private Integer partition;
	private Long totalOnce;
	
	private String hdfsUrl;
	private String sourceDriver;
	private String sourceJdbcUrl;
	private String sourcePwd;
	private String sourceUser;
	private Map<String,TableBean> tables;
	
	
	public String getHdfsUrl() {
		return hdfsUrl;
	}
	public void setHdfsUrl(String hdfsUrl) {
		this.hdfsUrl = hdfsUrl;
	}
	public Integer getPartition() {
		return partition;
	}
	public void setPartition(Integer partition) {
		this.partition = partition;
	}
	public Long getTotalOnce() {
		return totalOnce;
	}
	public void setTotalOnce(Long totalOnce) {
		this.totalOnce = totalOnce;
	}
	public String getSourceDriver() {
		return sourceDriver;
	}
	public void setSourceDriver(String sourceDriver) {
		this.sourceDriver = sourceDriver;
	}
	public String getSourceJdbcUrl() {
		return sourceJdbcUrl;
	}
	public void setSourceJdbcUrl(String sourceJdbcUrl) {
		this.sourceJdbcUrl = sourceJdbcUrl;
	}
	public String getSourcePwd() {
		return sourcePwd;
	}
	public void setSourcePwd(String sourcePwd) {
		this.sourcePwd = sourcePwd;
	}
	public String getSourceUser() {
		return sourceUser;
	}
	public void setSourceUser(String sourceUser) {
		this.sourceUser = sourceUser;
	}
	public Map<String, TableBean> getTables() {
		return tables;
	}
	public void setTables(Map<String, TableBean> tables) {
		this.tables = tables;
	}
	
	public static void main(String[] args){
		AccExportBean acc=new AccExportBean();
		
		acc.setPartition(100);
		acc.setSourceDriver("com.mysql.jdbc.Driver");
		acc.setSourceJdbcUrl("jdbc:mysql://10.8.202.215:3311/acc_export?useConfigs=maxPerformance&characterEncoding=utf8");
		acc.setSourcePwd("lppzacc");
		acc.setSourceUser("root");
		acc.setHdfsUrl("hdfs://hamaster:9000");
		acc.setTotalOnce(1500000l);
		acc.setTables(new HashMap<String,TableBean>());
		
//		TableBean memberTable=new TableBean();
//		memberTable.setBatchSize(300);
//		memberTable.setPartition(200);
//		memberTable.setPrimaryKey("_id");
//		memberTable.setSeqTable("lp_member_seq");
//		memberTable.setSourceTableOrView("lp_member_view");
//		memberTable.setTargetColumns(new ArrayList<String>(2));
//		memberTable.setTargetTableName(new ArrayList<String>(2));
//		memberTable.getTargetTableName().add("lp_member");
//		memberTable.getTargetTableName().add("lp_member_extend");
//		memberTable.getTargetColumns().add("partner_no,card_no,member_no,telphone,source_type,member_account,member_pwd,password_encoding,birthday,create_time,create_user,edit_time,edit_user,register_store,recommender,member_status,sync_crm_status,sync_crm_time,member_type,member_level");
//		memberTable.getTargetColumns().add("id,member_no,partner_no,member_name,member_nickname,gender,educationcd,occupation,marital_status,income,id_type,id_number,header_imgpath,create_time,delete_flag,register_ip,register_terminal,terminal_type,email");
//		memberTable.setSelectColumn(new HashMap<String,String>());
//		memberTable.getSelectColumn().put("lp_member", "partner_no,card_no,member_no,telephone,#{sourcetype},member_account,member_pwd,password_encoding,birthday,create_time,create_user,edit_time,edit_user,register_store,recommender,member_status,sync_crm_status,sync_crm_time,#{membercd},#{leveltype}");
//		memberTable.getSelectColumn().put("lp_member_extend", "id,member_no,partner_no,member_name,member_nickname,#{lpgendercd},#{lpeducationcd},#{lpjobcd},#{lpmarragecd},#{lpincomecd},#{lpidentitycd},id_number,header_imgpath,create_time,delete_flag,register_ip,register_terminal,terminal_type,email");
//		
//		memberTable.setJoinKey(new HashMap<String,ArrayList<String>>());
//		ArrayList<String> jk1=new ArrayList<String>();
//		jk1.add("membercd");
//		jk1.add("leveltype");
//		jk1.add("sourcetype");
//		memberTable.getJoinKey().put("lp_member", jk1);
//		
//		ArrayList<String> jk2=new ArrayList<String>();
//		jk2.add("lpgendercd");
//		jk2.add("lpeducationcd");
//		jk2.add("lpjobcd");
//		jk2.add("lpmarragecd");
//		jk2.add("lpincomecd");
//		jk2.add("lpidentitycd");
//		memberTable.getJoinKey().put("lp_member_extend", jk2);
//		
//		acc.getTables().put("1", memberTable);
		
		///////////////////////address/////////////////////////////////
//		TableBean addressTable=new TableBean();
//		addressTable.setBatchSize(300);
//		addressTable.setPartition(200);
//		addressTable.setPrimaryKey("_id");
//		addressTable.setSeqTable("lp_member_seq");
//		addressTable.setSourceTableOrView("lp_member_address_view");
//		addressTable.setTargetColumns(new ArrayList<String>(1));
//		addressTable.setTargetTableName(new ArrayList<String>(1));
//		addressTable.getTargetTableName().add("lp_member_address");
//		addressTable.getTargetColumns().add("id,member_no,partner_no,consignee,gender,position,longitude,dimensionality,map_type,address,cell_phone,province_code,province_name,city_code,city_name,district_code,district_name,create_time,edit_time,is_default,post_code");
//		addressTable.setSelectColumn(new HashMap<String,String>());
//		addressTable.getSelectColumn().put("lp_member_address", "id,member_no,partner_no,consignee,#{p_gender},position,longitude,dimensionality,map_type,address,cell_phone,province_code,province_name,city_code,city_name,district_code,district_name,create_time,edit_time,is_default,post_code");
//		addressTable.setJoinKey(new HashMap<String,ArrayList<String>>());
//		ArrayList<String> addjk=new ArrayList<String>();
//		addjk.add("p_gender");
//		addressTable.getJoinKey().put("lp_member_address", addjk);
//		acc.getTables().put("2", addressTable);
		
		///////////////////////lp_member_capital_account/////////////////////////////////
//		TableBean lmca=new TableBean();
//		lmca.setBatchSize(300);
//		lmca.setPartition(200);
//		lmca.setPrimaryKey("_id");
//		lmca.setSeqTable("lp_member_seq");
//		lmca.setSourceTableOrView("lp_member_capital_account_view");
//		lmca.setTargetColumns(new ArrayList<String>(1));
//		lmca.setTargetTableName(new ArrayList<String>(1));
//		lmca.getTargetTableName().add("lp_member_capital_account");
//		lmca.getTargetColumns().add("id,partner_no,member_no,card_no,level_type,all_scores,credit_scores,behavior_scores,all_card_balance,card_balance,donation_amount,card_status");
//		lmca.setSelectColumn(new HashMap<String,String>());
//		lmca.getSelectColumn().put("lp_member_capital_account", "id,partner_no,member_no,card_no,#{level_type},all_scores,credit_scores,behavior_scores,all_card_balance,card_balance,donation_amount,card_status");
//		lmca.setJoinKey(new HashMap<String,ArrayList<String>>());
//		ArrayList<String> lmcajk=new ArrayList<String>();
//		lmcajk.add("level_type");
//		lmca.getJoinKey().put("lp_member_capital_account", lmcajk);
//		acc.getTables().put("3", lmca);
		
		///////////////////////lp_member_capital_account/////////////////////////////////
//		TableBean lmca=new TableBean();
//		lmca.setBatchSize(300);
//		lmca.setPartition(200);
//		lmca.setPrimaryKey("_id");
//		lmca.setSeqTable("lp_member_seq");
//		lmca.setSourceTableOrView("lp_member_rela_accounts_view");
//		lmca.setTargetColumns(new ArrayList<String>(1));
//		lmca.setTargetTableName(new ArrayList<String>(1));
//		lmca.getTargetTableName().add("lp_member_rela_accounts");
//		lmca.getTargetColumns().add("id,member_no,partner_no,chanel_code,outer_user_id,outer_unionid,create_time,delete_flag");
//		lmca.setSelectColumn(new HashMap<String,String>());
//		lmca.getSelectColumn().put("lp_member_rela_accounts", "id,member_no,partner_no,chanel_code,outer_user_id,outer_unionid,create_time,delete_flag");
//		acc.getTables().put("4", lmca);
		
		///////////////////////lpmarketgrouprelamembers/////////////////////////////////
//		TableBean lpmarketgrouprelamembers=new TableBean();
//		lpmarketgrouprelamembers.setBatchSize(300);
//		lpmarketgrouprelamembers.setPartition(200);
//		lpmarketgrouprelamembers.setPrimaryKey("_id");
//		lpmarketgrouprelamembers.setSeqTable("lp_member_seq");
//		lpmarketgrouprelamembers.setSourceTableOrView("lp_marketgrouprelamembers_view");
//		lpmarketgrouprelamembers.setTargetColumns(new ArrayList<String>(1));
//		lpmarketgrouprelamembers.setTargetTableName(new ArrayList<String>(1));
//		lpmarketgrouprelamembers.getTargetTableName().add("lp_marketgrouprelamembers");
//		lpmarketgrouprelamembers.getTargetColumns().add("id,lpgroupid,lpmemberno");
//		lpmarketgrouprelamembers.setSelectColumn(new HashMap<String,String>());
//		lpmarketgrouprelamembers.getSelectColumn().put("lp_marketgrouprelamembers", "id,lpgroupid,lpmemberno");
//		acc.getTables().put("5", lpmarketgrouprelamembers);
		
		///////////////////////lp_member_change_details/////////////////////////////////
		TableBean lp_member_change_details=new TableBean();
		lp_member_change_details.setBatchSize(300);
		lp_member_change_details.setPartition(200);
		lp_member_change_details.setPrimaryKey("_id");
		lp_member_change_details.setSeqTable("lp_member_seq");
		lp_member_change_details.setSourceTableOrView("lp_member_change_details_view");
		lp_member_change_details.setTargetColumns(new ArrayList<String>(1));
		lp_member_change_details.setTargetTableName(new ArrayList<String>(1));
		lp_member_change_details.getTargetTableName().add("lp_member_change_details");
		lp_member_change_details.getTargetColumns().add("id,member_no,partner_no,change_type,member_type,points,pk_crm,outorder_code,order_code,line_id,txn_reason,remark,create_time");
		lp_member_change_details.setSelectColumn(new HashMap<String,String>());
		lp_member_change_details.getSelectColumn().put("lp_member_change_details", "id,member_no,partner_no,change_type,#membertypecd,points,pk_crm,outorder_code,order_code,line_id,txn_reason,remark,create_time");
		acc.getTables().put("6", lp_member_change_details);
		Yaml y=new Yaml();
		
		System.out.println(y.dump(acc));
	}
}
