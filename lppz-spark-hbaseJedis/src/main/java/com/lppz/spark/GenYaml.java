package com.lppz.spark;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.fastjson.JSON;
import com.lppz.spark.bean.SparkSqlDmlBean;
import com.lppz.spark.scala.bean.HBaseBean;
import com.lppz.spark.scala.bean.Spark2HbaseBean;

public class GenYaml {

	public static void  gen(){
		InputStream is=null;
        Workbook wb=null;
        
        try {
			is = new FileInputStream(new File("/Users/zoubin/Documents/scalawork/lppz-spark-biz/lppz-spark-hbaseJedis/src/test/resources/META-INF/spark2hbase11Up.xlsx"));
			wb = WorkbookFactory.create(is);
		} catch (InvalidFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
        
        Sheet sheet = wb.getSheetAt(0);
        
        Row row = null;
        
        Spark2HbaseBean root=null;
        
        for (int i = 1; i<=sheet.getLastRowNum(); i++) {
        	row = sheet.getRow(i);
        	
        	if(1==i){
            	root=new Spark2HbaseBean();
            	root.setSparkBeanMap(new HashMap<String,Spark2HbaseBean>());
            	root.setSqlBean(buildSqlBean(row));
            	root.setTargetBean(buildTargetBean(row));
        	}else{
        		Spark2HbaseBean temp=findNode(root,row);
        		
        		Spark2HbaseBean children=new Spark2HbaseBean();
        		children.setSparkBeanMap(new HashMap<String,Spark2HbaseBean>());
        		children.setSqlBean(buildSqlBean(row));
        		children.setTargetBean(buildTargetBean(row));
        		
        		if(null==temp){
        			root.getSparkBeanMap().put(children.getTargetBean().getCf(), children);
        		}else{
        			temp.getSparkBeanMap().put(children.getTargetBean().getCf(), children);
        		}
        	}
        }
        String str=new Yaml().dump(root);
        try {
			FileWriter fw=new FileWriter(new File("/Users/zoubin/Documents/scalawork/lppz-spark-biz/lppz-spark-hbaseJedis/src/main/resources/META-INF/spark2hbaseAll.yaml"));
			fw.write(str);
			fw.flush();
			fw.close();
        } catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void reverse()throws Exception{
		File file=new File("C:\\Users\\romeo\\Desktop\\spark2hbase.yaml");
		
		Yaml y=new Yaml();
		
		Spark2HbaseBean bean=y.loadAs(new FileInputStream(file),Spark2HbaseBean.class);
		
		System.out.println(JSON.toJSONString(bean));
	}
	
	public static Spark2HbaseBean findNode(Spark2HbaseBean bean,Row row){
		if(null ==bean.getSparkBeanMap() || null == row)
			return null;
		
		Map<String,Spark2HbaseBean> map=bean.getSparkBeanMap();
		
		for(Entry<String,Spark2HbaseBean> e:map.entrySet()){
			if(e.getKey().equals(row.getCell(7).getStringCellValue())){
				return e.getValue();
			}else{
				Spark2HbaseBean temp= findNode(e.getValue(),row);
				
				if(null!=temp)
					return temp;
			}
		}
		
		
		return null;
	}
	
	public static SparkSqlDmlBean buildSqlBean(Row row){
		if(null==row)
			return null;
		
		SparkSqlDmlBean sqlBean=new SparkSqlDmlBean();
    	sqlBean.setTableName(row.getCell(15).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(15).getStringCellValue());
    	sqlBean.setSchema(row.getCell(1).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(1).getStringCellValue());
    	sqlBean.setRelateKey(row.getCell(2).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(2).getStringCellValue());
    	sqlBean.setTotal4Once(Double.valueOf(row.getCell(3).getNumericCellValue()).longValue());
    	sqlBean.setSql(row.getCell(4).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(4).getStringCellValue());
    	sqlBean.setColList(row.getCell(5).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(5).getStringCellValue());
    	sqlBean.setMainIdColumn(row.getCell(6).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(6).getStringCellValue());
    	sqlBean.setParentFamilyName(row.getCell(7).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(7).getStringCellValue());
    	sqlBean.setExcludeColumnList(row.getCell(8).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(8).getStringCellValue());
    	sqlBean.setUidList(row.getCell(9).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(9).getStringCellValue());
    	sqlBean.setIsRootHbase(row.getCell(10).getBooleanCellValue());
    	sqlBean.setIsLeaf(row.getCell(11).getBooleanCellValue());
    	sqlBean.setFamilyName(row.getCell(12).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(12).getStringCellValue());
    	
    	return sqlBean;
	}
	
	public static HBaseBean buildTargetBean(Row row){
		if(null==row)
			return null;
		
		HBaseBean targetBean=new HBaseBean();
    	targetBean.setHbaseQuorum(row.getCell(13).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(13).getStringCellValue());
    	targetBean.setHbasePort(Double.valueOf(row.getCell(14).getNumericCellValue()).intValue()+"");
    	targetBean.setTablebname(row.getCell(15).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(15).getStringCellValue());
    	targetBean.setCf(row.getCell(16).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(16).getStringCellValue());
    	targetBean.setPartionNum(Double.valueOf(row.getCell(17).getNumericCellValue()).intValue());
    	targetBean.setIsWal(row.getCell(18).getBooleanCellValue());
    	
    	return targetBean;
	}
	
	public static void main(String[] args) {
		gen();
	}
}
