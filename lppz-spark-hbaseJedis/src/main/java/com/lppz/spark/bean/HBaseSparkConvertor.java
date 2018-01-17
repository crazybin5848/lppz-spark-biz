package com.lppz.spark.bean;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import com.alibaba.fastjson.JSON;
import com.lppz.spark.scala.hbase.HbaseAbstractSparkConv;
import com.lppz.spark.util.jedis.SparkJedisCluster;
import com.lppz.util.kryo.KryoUtil;

public class HBaseSparkConvertor extends HbaseAbstractSparkConv{

	public HBaseSparkConvertor() {
		super(null,null,null);
	}
	public HBaseSparkConvertor(Boolean isRootHbase,String mainIdColumn,Boolean isLeaf,String tableName,String familyName,String parentFamilyName,String relateKey,String colList,String hbaseQuorum,String hbasePort,Jedis jedis) {
		this();
		this.parentFamilyName=parentFamilyName;
		this.familyName=familyName;
		this.isRootHbase=isRootHbase;
		this.mainIdColumn=mainIdColumn;
		this.isLeaf=isLeaf;
		this.colList=colList;
		this.tableName=tableName;
		this.relateKey=relateKey;
		init(hbaseQuorum, hbasePort,jedis);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 112312312344L;
	private Boolean isRootHbase;
	private String mainIdColumn;
	private Boolean isLeaf;
	private String colList;
	private String tableName;
	private String familyName;
	private String parentFamilyName;
	private String relateKey;
	private Map<String,RowKeyComposition> mapColCache;
	private Map<String,HBaseMeta> mapCache;
	
	public synchronized void init(String hbaseQuorum,String hbasePort,Jedis jedis) {
		if(mapColCache==null){
			byte[] mapColByte=jedis.hget("mapHbaseColCache".getBytes(), "mapColCache".getBytes());
			byte[] mapCacheByte=jedis.hget("mapHbaseColCache".getBytes(), "mapCacheByte".getBytes());
			if(mapColByte!=null){
				try {
					mapColCache=KryoUtil.kyroDeSeriLize(mapColByte,Map.class);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if(mapCacheByte!=null){
				try {
					mapCache=KryoUtil.kyroDeSeriLize(mapCacheByte,Map.class);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if(mapColCache!=null&&mapCache!=null)
				return;
			try {
				Configuration conf = HBaseConfiguration.create();
				conf.set("hbase.zookeeper.property.clientPort", hbasePort);  
				conf.set("hbase.zookeeper.quorum", hbaseQuorum);
				conf.setLong("hbase.rpc.timeout", 60000000);
				conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 60000000);
				conf.setLong("hbase.client.scanner.caching", 30);
				HBaseAdmin admin=new HBaseAdmin(conf);
				initMapCache(admin);
				byte[] b1=KryoUtil.kyroSeriLize(mapColCache, -1);
				byte[] b2=KryoUtil.kyroSeriLize(mapCache, -1);
				jedis.hset("mapHbaseColCache".getBytes(), "mapColCache".getBytes(),b1);
				jedis.hset("mapHbaseColCache".getBytes(), "mapCacheByte".getBytes(),b2);
			} catch (IOException e) {
				super.LOG().error(e.getMessage(),e);
//				e.printStackTrace();
			}
		}
	}
	
	private void initMapCache(HBaseAdmin admin) {
		HTableDescriptor[] hdts=null;
		try {
			hdts = admin.listTables();
		} catch (IOException e) {
			super.LOG().error(e.getMessage(),e);
		}
		if(hdts==null||hdts.length==0)
			return;
		mapColCache=new HashMap<String,RowKeyComposition>(hdts.length);
		mapCache=new HashMap<String,HBaseMeta>();
		for(HTableDescriptor hdt:hdts){
			if(!hdt.getNameAsString().startsWith(Constants.IDXTABLENAMEESPREFIX))
			{
				String regionNum=hdt.getValue(Constants.HBASEREGIONNUM);
				@SuppressWarnings("unchecked")
				Map<String,String> regionMap=JSON.parseObject(hdt.getValue(Constants.HBASEJEDISTABLEMAP), Map.class);
				mapCache.put(hdt.getNameAsString(), new HBaseMeta(regionNum,regionMap));
				for(HColumnDescriptor hcd:hdt.getColumnFamilies()){
					RowKeyComposition rkc=JSON.parseObject(hdt.getValue(hcd.getNameAsString()), RowKeyComposition.class);
					mapColCache.put(hdt.getNameAsString()+":::"+hcd.getNameAsString(), rkc);
				}
			}
		}
	}
	
	@Override
	public String buildRowKey(HashMap<String, String> qulifierAndValue) {
		StringBuilder sb;
		try {
			String prefix=null;
			JedisHbaseBean jedisBean=null;
			RowKeyComposition rkc=mapColCache.get(this.tableName+":::"+this.familyName);
			if(isRootHbase){
//				SparkJedisCluster jedis=super.getJedisCluster();
				String orgMainId=qulifierAndValue.get(rkc.getMainHbaseCfPk().getOidQulifierName());
//				String mainId=rkc.getMainHbaseCfPk().buidFixedLengthNumber(orgMainId);
				prefix=HbaseUtil.buildPrefixByMainId(orgMainId);
//				String kk=this.tableName+"--"+this.familyName+"--"+mainId;
//				prefix=jedis.get(kk);
//				if(StringUtils.isBlank(prefix)){
//					prefix=buildSequencePrefix();
//					jedis.set(kk, prefix);
//				}
			}
			else{
					jedisBean=getJedisHBaseBeanFromJedis(qulifierAndValue);
					if (jedisBean == null) {
						return null;
					}
					prefix=jedisBean.getPrefix();
			}
			sb = new StringBuilder(prefix).append(Constants.REGTABLEHBASEPREFIIX);
			
			if(rkc.getOrderBy()!=null){
				if(!qulifierAndValue.containsKey(rkc.getOrderBy().getQulifier()))
					throw new IllegalStateException("Col Value Map must contain:"+rkc.getOrderBy().getQulifier()); 
				String orderByValue=rkc.getOrderBy().build(qulifierAndValue.get(rkc.getOrderBy().getQulifier()));
				if(orderByValue!=null)
					sb.//append(rkc.getOrderBy().getQulifier()).append(Constants.QSPLITTERORDERBY).
					append(orderByValue).append(Constants.QLIFIERSPLITTER);
			}
			TreeSet<String> colKeysForRow=rkc.getFamilyColsForRowKey();
			int i=0;
			for(String q:colKeysForRow){
				String v=qulifierAndValue.get(q);
				if(v==null) throw new IllegalStateException("Col Value Map must contain:"+q); 
				if(rkc.getMainHbaseCfPk().getOidQulifierName().equals(q)){
					String mainId=rkc.getMainHbaseCfPk().buidFixedLengthNumber(v);
					sb.append(q).append(Constants.QSPLITTER).append(mainId);
				}
				else
				sb.append(q).append(Constants.QSPLITTER).append(v);
				if(i++<colKeysForRow.size()-1)
					sb.append(Constants.SPLITTER);
			}
			if(isRootHbase){
				try {
					saveRootHbaseRedis(prefix, qulifierAndValue,rkc);
				} catch (IOException e) {
					super.LOG().error(e.getMessage(),e);
				}
			}
			else {
				appendCascadeRowKeyByJedis(jedisBean, sb);
				if(this.relateKey!=null)
				qulifierAndValue.remove(relateKey);
				if(jedisBean.getParentId()!=null){
					String[] pp=jedisBean.getOrgParentId().split(Constants.QSPLITTER);
					qulifierAndValue.put(pp[0], pp[1]);
				}
				String[] mm=jedisBean.getOrgMainId().split(Constants.QSPLITTER);
				qulifierAndValue.put(mm[0], mm[1]);
				if(!isLeaf){
					try {
						saveMiddleHbaseRedis(qulifierAndValue, prefix, jedisBean,rkc);
					} catch (IOException e) {
						super.LOG().error(e.getMessage(),e);
					}
				}
			}
		} catch (Exception e) {
			super.LOG().error(e.getMessage(),e);
			throw e;
		}
		return sb.toString();
	}
	private void appendCascadeRowKeyByJedis(JedisHbaseBean jedisBean,
			StringBuilder sb) {
		if(jedisBean.getParentId()!=null)
		sb.append(Constants.SPLITTER).append(jedisBean.getParentId());
		sb.append(Constants.SPLITTER).append(jedisBean.getMainId());
	}
	
	private void saveMiddleHbaseRedis(Map<String, String> qulifierAndValue,
			String prefix, JedisHbaseBean jedisBean, RowKeyComposition rkc) throws IOException {
		SparkJedisCluster jedis=super.getJedisCluster();
		String mid=jedisBean.getMainId();
		String parentId=new StringBuilder(this.mainIdColumn).append(Constants.QSPLITTER)
		.append(rkc.getMainHbaseCfPk().buidFixedLengthNumber(qulifierAndValue.get(this.mainIdColumn))).toString();
		JedisHbaseBean jjhba=new JedisHbaseBean(prefix,mid,parentId);
		jjhba.setOrgMainId(jedisBean.getOrgMainId());
		jjhba.setOrgParentId(new StringBuilder(this.mainIdColumn).append(Constants.QSPLITTER).append(qulifierAndValue.get(this.mainIdColumn)).toString());
		saveJedisHbase(qulifierAndValue, jedis, jjhba);
	}
	private void saveJedisHbase(Map<String, String> qulifierAndValue,
			SparkJedisCluster jedis, JedisHbaseBean jjhba) throws IOException {
			//sub table join parant table columns,just deal root table
		for(String col:this.colList.split(",")){
			String jedisKey=new StringBuilder("")
						.append(this.familyName)
						.append(":").append(qulifierAndValue.get(col))
						.toString();
			jedis.set(jedisKey.getBytes(), KryoUtil.kyroSeriLize(jjhba, -1));
		}
	}
	
	private void saveRootHbaseRedis(String prefix,
			Map<String, String> qulifierAndValue, RowKeyComposition rkc)
			throws IOException {
		SparkJedisCluster jedis=super.getJedisCluster();
		String mid=new StringBuilder(this.mainIdColumn).append(Constants.QSPLITTER)
				.append(rkc.getMainHbaseCfPk().buidFixedLengthNumber(qulifierAndValue.get(this.mainIdColumn))).toString();
		JedisHbaseBean jhbb=new JedisHbaseBean(prefix,mid,null);
		jhbb.setOrgMainId(new StringBuilder(this.mainIdColumn).append(Constants.QSPLITTER).append(qulifierAndValue.get(this.mainIdColumn)).toString());
		saveJedisHbase(qulifierAndValue, jedis, jhbb);
	}
	
	public String buildSequencePrefix() {
		String regionNum=mapCache.get(this.tableName).getRegionNum();
		Map<String,String> regionMap=mapCache.get(this.tableName).getRegionMap();
		String kk=generateSequencePrefixRowKey(regionNum==null?1:Integer.parseInt(regionNum),regionMap);
		if(kk==null)
			throw new IllegalStateException("lack of meta Table data:"+tableName);
		return kk;
	}
	
	private String generateSequencePrefixRowKey(int length, Map<String, String> regionMap) {
		if(length>36||length<1)
			throw new IllegalStateException(tableName+"'s pre region num can not gt 36 or le 1"); 
		Jedis jedis=super.getJedis();
		String x=Long.toString(Math.abs(UUID.randomUUID().getMostSignificantBits() % length)+1,36);
		StringBuilder sb=new StringBuilder("10".equals(x)?"0":x);
		String hkey=regionMap.get(sb.toString());
		if(hkey==null)
			hkey=regionMap.get("");
		Long num=jedis.hincrBy(tableName+"_"+this.familyName, hkey, 1);
		String ssnum="";
		try {
			ssnum = HbaseUtil.addZeroForNum(Long.toString(num,36), 8);
		} catch (Exception e) {
			super.LOG().error(e.getMessage(),e);
		}
		sb.append(ssnum);
		return sb.toString();
	}
	
	private JedisHbaseBean getJedisHBaseBeanFromJedis(Map<String,String> rowMap) {
		SparkJedisCluster jedis=super.getJedisCluster();
		String val=rowMap.get(this.relateKey);
		String jedisKey=new StringBuilder("")
				.append(this.parentFamilyName)
				.append(":").append(val).toString();
		byte[] jhb=jedis.get(jedisKey.getBytes());
		if(jhb==null)
			return null;
		try {
			JedisHbaseBean jhbb=KryoUtil.kyroDeSeriLize(jhb, JedisHbaseBean.class);
//			if(jhbb.getMainId()==null)
//				jhbb.setMainId(val);
			return jhbb;
		} catch (Exception e) {
			super.LOG().error(e.getMessage(),e);
		}
		return null;
	}
	
	public Boolean getIsRootHbase() {
		return isRootHbase;
	}
	public void setIsRootHbase(Boolean isRootHbase) {
		this.isRootHbase = isRootHbase;
	}
	public String getMainIdColumn() {
		return mainIdColumn;
	}
	public void setMainIdColumn(String mainIdColumn) {
		this.mainIdColumn = mainIdColumn;
	}
	public Boolean getIsLeaf() {
		return isLeaf;
	}
	public void setIsLeaf(Boolean isLeaf) {
		this.isLeaf = isLeaf;
	}
	public String getColList() {
		return colList;
	}
	public void setColList(String colList) {
		this.colList = colList;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getFamilyName() {
		return familyName;
	}
	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}
	public String getParentFamilyName() {
		return parentFamilyName;
	}
	public void setParentFamilyName(String parentFamilyName) {
		this.parentFamilyName = parentFamilyName;
	}
	public String getRelateKey() {
		return relateKey;
	}
	public void setRelateKey(String relateKey) {
		this.relateKey = relateKey;
	}
	public Map<String, RowKeyComposition> getMapColCache() {
		return mapColCache;
	}
	public void setMapColCache(Map<String, RowKeyComposition> mapColCache) {
		this.mapColCache = mapColCache;
	}
	public Map<String, HBaseMeta> getMapCache() {
		return mapCache;
	}
	public void setMapCache(Map<String, HBaseMeta> mapCache) {
		this.mapCache = mapCache;
	}
}