package com.lppz.spark.sap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.fastjson.JSON;
import com.lppz.spark.bean.SparkSqlDmlBean;
import com.lppz.spark.scala.bean.HBaseBean;
import com.lppz.spark.scala.bean.Spark2HbaseBean;

public class GenYaml {
	
	private static int i=1;

	@Test
	public void gen(){
		InputStream is=null;
        Workbook wb=null;
        
        try {
			is = new FileInputStream(new File("C:\\Users\\romeo\\Desktop\\config\\spark2hbaseNew.xlsx"));
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
			FileWriter fw=new FileWriter(new File("C:\\Users\\romeo\\Desktop\\config\\spark2hbase.yaml"));
			fw.write(str);
			fw.flush();
			fw.close();
        } catch (IOException e) {
			e.printStackTrace();
		}
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
    	sqlBean.setTableName(row.getCell(0).getStringCellValue().equalsIgnoreCase("null")?null:row.getCell(0).getStringCellValue());
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
	
	@Test
	public void reverse()throws Exception{
		File file=new File("F:\\workspace\\lppz-spark-biz\\lppz-spark-hbaseJedis\\src\\main\\resources\\META-INF\\spark2hbaseAll.yaml");
		
		Yaml y=new Yaml();
		
		Spark2HbaseBean bean=y.loadAs(new FileInputStream(file),Spark2HbaseBean.class);
		
		System.out.println(JSON.toJSONString(bean));
		
		// 第一步，创建一个webbook，对应一个Excel文件  
		XSSFWorkbook wb = new XSSFWorkbook();  
        // 第二步，在webbook中添加一个sheet,对应Excel文件中的sheet  
		XSSFSheet  sheet = wb.createSheet("ok");  
        // 第三步，在sheet中添加表头第0行,注意老版本poi对Excel的行数列数有限制short  
		XSSFRow row = sheet.createRow((int) 0);  
        // 第四步，创建单元格，并设置值表头 设置表头居中  
		XSSFCellStyle style = wb.createCellStyle();  
        style.setAlignment(HSSFCellStyle.ALIGN_CENTER); // 创建一个居中格式
        
        buildExecl(row,bean);
        
        gogogo(bean.getSparkBeanMap(),sheet);
        
        OutputStream out=new FileOutputStream("C:\\Users\\romeo\\Desktop\\config\\spark2hbaseNew.xlsx");
        
        wb.write(out);
        
        out.flush();
        
        out.close();
	}
	
	public static void gogogo(Map<String,Spark2HbaseBean> map,XSSFSheet sheet){
		if(null!=map && !map.isEmpty()){
			for(Entry<String,Spark2HbaseBean> entry:map.entrySet()){
				buildExecl(sheet.createRow(i),entry.getValue());
				
				i++;
				
				if(entry.getValue().getSparkBeanMap()!=null && !entry.getValue().getSparkBeanMap().isEmpty()){
					gogogo(entry.getValue().getSparkBeanMap(),sheet);
				}
			}
		}
	}
	
	public static void buildExecl(XSSFRow row,Spark2HbaseBean bean){
		row.createCell(0).setCellValue(bean.getSqlBean().getTableName()==null?"null":bean.getSqlBean().getTableName());
        row.createCell(1).setCellValue(bean.getSqlBean().getSchema()==null?"null":bean.getSqlBean().getSchema());
        row.createCell(2).setCellValue(bean.getSqlBean().getRelateKey()==null?"null":bean.getSqlBean().getRelateKey());
        row.createCell(3).setCellValue(bean.getSqlBean().getTotal4Once());
        row.createCell(4).setCellValue(bean.getSqlBean().getSql()==null?"null":bean.getSqlBean().getSql());
        row.createCell(5).setCellValue(bean.getSqlBean().getColList()==null?"null":bean.getSqlBean().getColList());
        row.createCell(6).setCellValue(bean.getSqlBean().getMainIdColumn()==null?"null":bean.getSqlBean().getMainIdColumn());
        row.createCell(7).setCellValue(bean.getSqlBean().getParentFamilyName()==null?"null":bean.getSqlBean().getParentFamilyName());
        row.createCell(8).setCellValue(bean.getSqlBean().getExcludeColumnList()==null?"null":bean.getSqlBean().getExcludeColumnList());
        row.createCell(9).setCellValue(bean.getSqlBean().getUidList()==null?"null":bean.getSqlBean().getUidList());
        row.createCell(10).setCellValue(bean.getSqlBean().getIsRootHbase());
        row.createCell(11).setCellValue(bean.getSqlBean().getIsLeaf());
        row.createCell(12).setCellValue(bean.getSqlBean().getFamilyName()==null?"null":bean.getSqlBean().getFamilyName());
        
        row.createCell(13).setCellValue(bean.getTargetBean().getHbaseQuorum()==null?"null":bean.getTargetBean().getHbaseQuorum());
        row.createCell(14).setCellValue(Integer.parseInt(bean.getTargetBean().getHbasePort()));
        row.createCell(15).setCellValue(bean.getTargetBean().getTablebname()==null?"null":bean.getTargetBean().getTablebname());
        row.createCell(16).setCellValue(bean.getTargetBean().getCf()==null?"null":bean.getTargetBean().getCf());
        row.createCell(17).setCellValue(bean.getTargetBean().getPartionNum());
        row.createCell(18).setCellValue(bean.getTargetBean().getIsWal());
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
}
