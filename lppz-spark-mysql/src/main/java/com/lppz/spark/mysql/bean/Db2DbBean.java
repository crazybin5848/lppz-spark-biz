package com.lppz.spark.mysql.bean;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class Db2DbBean implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -9208293735763983537L;
	private String sourceJdbcUrl;
	private String sourceUser;
	private String sourcePwd;
	private String sourceDriver;
	private String targetJdbcUrl;
	private String targetUser;
	private String targetPwd;
	private String targetDriver;
	private Long totalOnce;
	private Integer partition;
	private Map<String,Db2DbTableBean> tables;
	private String kafkaBrokerPath;
	
	public String getKafkaBrokerPath() {
		return kafkaBrokerPath;
	}

	public void setKafkaBrokerPath(String kafkaBrokerPath) {
		this.kafkaBrokerPath = kafkaBrokerPath;
	}

	public String getSourceJdbcUrl() {
		return sourceJdbcUrl;
	}

	public void setSourceJdbcUrl(String sourceJdbcUrl) {
		this.sourceJdbcUrl = sourceJdbcUrl;
	}

	public String getSourceUser() {
		return sourceUser;
	}

	public void setSourceUser(String sourceUser) {
		this.sourceUser = sourceUser;
	}

	public String getSourcePwd() {
		return sourcePwd;
	}

	public void setSourcePwd(String sourcePwd) {
		this.sourcePwd = sourcePwd;
	}

	public String getSourceDriver() {
		return sourceDriver;
	}

	public void setSourceDriver(String sourceDriver) {
		this.sourceDriver = sourceDriver;
	}

	public String getTargetJdbcUrl() {
		return targetJdbcUrl;
	}

	public void setTargetJdbcUrl(String targetJdbcUrl) {
		this.targetJdbcUrl = targetJdbcUrl;
	}

	public String getTargetUser() {
		return targetUser;
	}

	public void setTargetUser(String targetUser) {
		this.targetUser = targetUser;
	}

	public String getTargetPwd() {
		return targetPwd;
	}

	public void setTargetPwd(String targetPwd) {
		this.targetPwd = targetPwd;
	}

	public String getTargetDriver() {
		return targetDriver;
	}

	public void setTargetDriver(String targetDriver) {
		this.targetDriver = targetDriver;
	}

	public Long getTotalOnce() {
		return totalOnce;
	}

	public void setTotalOnce(Long totalOnce) {
		this.totalOnce = totalOnce;
	}

	public Integer getPartition() {
		return partition;
	}

	public void setPartition(Integer partition) {
		this.partition = partition;
	}

	public Map<String,Db2DbTableBean> getTables() {
		return tables;
	}

	public void setTables(Map<String,Db2DbTableBean> tables) {
		this.tables = tables;
	}

	public static void main(String[] args)throws Exception{

		buildYamlFile();
//		loadYaml();
		
	}
	
	private static void loadYaml(){
		File file=new File("/home/licheng/accToMycat.yaml");
		Yaml y=new Yaml();
		Db2DbBean bean;
		try {
			bean = y.loadAs(new FileInputStream(file), Db2DbBean.class);
			System.out.println(y.dump(bean));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		
	}
	
	private static void buildYamlFile(){
		String jdbcurl = "jdbc:mysql://10.8.202.215:3311/acc_export?useConfigs=maxPerformance&characterEncoding=utf8";
		String user = "root";
		String pwd = "lppzacc";
		int partition = 500;
		int batchSize = 100;
		String startTime = "2015-05-01";
		String endTime = "2015-08-01";
		Db2DbBean bbean = new Db2DbBean();
		bbean.setPartition(10);
//		bbean.setSourceDriver("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
		bbean.setSourceDriver("com.mysql.jdbc.Driver");
		bbean.setSourceJdbcUrl(jdbcurl);
		bbean.setSourceUser(user);
		bbean.setSourcePwd(pwd);
		bbean.setTotalOnce(1000L);
//		bbean.setTables(new ArrayList<Db2DbTableBean>());
		bbean.setTables(new HashMap<String,Db2DbTableBean>());
		
		Db2DbTableBean memberBean = new Db2DbTableBean();
		memberBean.setPrimaryKey("id");
		memberBean.setTargetColumns("partner_no,card_no,member_no,telphone,source_type,member_account,member_pwd,password_encoding,birthday,create_time,create_user,edit_time,edit_user,register_store,recommender,member_status,sync_crm_status,sync_crm_time,member_type,member_level");
		memberBean.setPartition(partition);
		memberBean.setBatchSize(batchSize);
		memberBean.setTargetTableName("lp_member");
		memberBean.setSeqTable("lp_member_seq");
		memberBean.setSourceTableOrView("lp_member_view");
		bbean.getTables().put("1", memberBean);
		
//		Db2DbTableBean memberBean2 = new Db2DbTableBean();
//		memberBean2.setSourceSql("SELECT a.p_lppartnerno as partner_no, a.p_lpcardno as card_no, b.p_memberno as member_no, a.p_lpmobileno as telephone, e.`Code` AS source_type, a.p_uid as member_account, a.passwd as member_pwd, a.p_passwordencoding as password_encoding, a.p_lpbirthday as birthday, a.createdTS as create_time, a.p_lpcreator as create_user, a.modifiedTS as edit_time, 'admin' as edit_user, a.p_lpregisterstore as register_store, a.p_lprecomder as recommender, a.p_lpmemberstatus as member_status, a.p_synccrmstatus as sync_crm_status, a.p_synccrmtime as sync_crm_time, c.`Code` AS member_type, d.`Code` AS member_level FROM lpmembers b INNER JOIN users a ON b.p_customer =a.PK LEFT JOIN enumerationvalues c ON c.PK = b.p_membercd LEFT JOIN enumerationvalues d ON d.PK = b.p_leveltype LEFT JOIN enumerationvalues e ON e.PK = b.p_sourcetype where a.createdts >=? and a.createdts<?");
//		memberBean2.setPrimaryKey("member_no");
//		memberBean2.setTargetColumns("partner_no,card_no,member_no,telphone,source_type,member_account,member_pwd,password_encoding,birthday,create_time,create_user,edit_time,edit_user,register_store,recommender,member_status,sync_crm_status,sync_crm_time,member_type,member_level");
//		memberBean2.setStartTag("2016-01-01");
//		memberBean2.setEndTag("2016-03-31");
//		memberBean2.setPartition(100);
//		memberBean2.setBatchSize(500);
//		memberBean2.setTargetTableName("lp_member");
//		bbean.getTables().add(memberBean2);
		
		Db2DbTableBean memberAddrBean = new Db2DbTableBean();
		memberAddrBean.setTargetColumns("id,member_no,partner_no,consignee,gender,position,longitude,dimensionality,map_type,address,cell_phone,province_code,province_name,city_code,city_name,district_code,district_name,create_time,edit_time,is_default,post_code");
		memberAddrBean.setPartition(partition);
		memberAddrBean.setBatchSize(batchSize);
		memberAddrBean.setStartTag(startTime);
		memberAddrBean.setEndTag(endTime);
		memberAddrBean.setTargetTableName("lp_member_address");
		memberAddrBean.setSeqTable("lp_member_seq");
		memberAddrBean.setSourceTableOrView("lp_member_address_view");
		memberAddrBean.setPrimaryKey("_id");
		bbean.getTables().put("2", memberAddrBean);
		
		Db2DbTableBean memberExtendBean = new Db2DbTableBean();
		memberExtendBean.setTargetColumns("id,member_no,partner_no,member_name,member_nickname,gender,educationcd,occupation,marital_status,income,id_type,id_number,header_imgpath,create_time,delete_flag,register_ip,register_terminal,terminal_type,email");
		memberExtendBean.setPartition(partition);
		memberExtendBean.setBatchSize(batchSize);
		memberExtendBean.setStartTag(startTime);
		memberExtendBean.setEndTag(endTime);
		memberExtendBean.setTargetTableName("lp_member_extend");
		memberExtendBean.setSeqTable("lp_member_seq");
		memberExtendBean.setSourceTableOrView("lp_member_extend_view");
		memberExtendBean.setPrimaryKey("_id");
		bbean.getTables().put("3", memberExtendBean);
		
		Db2DbTableBean memberCapitalAccountBean = new Db2DbTableBean();
		memberCapitalAccountBean.setPartition(partition);
		memberCapitalAccountBean.setBatchSize(batchSize);
		memberCapitalAccountBean.setStartTag(startTime);
		memberCapitalAccountBean.setEndTag(endTime);
		memberCapitalAccountBean.setTargetTableName("lp_member_capital_account");
		memberCapitalAccountBean.setSeqTable("lp_member_seq");
		memberCapitalAccountBean.setSourceTableOrView("lp_member_capital_account_view");
		memberCapitalAccountBean.setPrimaryKey("_id");
		bbean.getTables().put("4", memberCapitalAccountBean);
		
		Db2DbTableBean memberRelaAccountBean = new Db2DbTableBean();
		memberRelaAccountBean.setParentKey("c.p_memberno");
		memberRelaAccountBean.setSourceSql("SELECT a.PK as id, c.p_memberno as member_no, b.p_lppartnerno as partner_no, NULL AS chanel_code, a.p_outerid as outer_user_id, d.p_unionId AS outer_unionid, a.createdTS as create_time, '0' AS delete_flag from lprelaaccounts a INNER JOIN users b ON b.PK = a.p_customer INNER JOIN lpmembers c ON c.p_customer =a.p_customer INNER JOIN lpwechatopenidunionidrel d ON d.p_openId=a.p_outerid where b.createdts >=? and b.createdts<?");
		memberRelaAccountBean.setTargetColumns("id,member_no,partner_no,chanel_code,outer_user_id,outer_unionid,create_time,delete_flag");
		memberRelaAccountBean.setPartition(partition);
		memberRelaAccountBean.setBatchSize(batchSize);
		memberRelaAccountBean.setStartTag(startTime);
		memberRelaAccountBean.setEndTag(endTime);
		memberRelaAccountBean.setTargetTableName("lp_member_rela_accounts");
//		bbean.getTables().add(memberRelaAccountBean);
		
		Yaml y=new Yaml();
		
		System.out.println(y.dump(bbean));
	}
}
