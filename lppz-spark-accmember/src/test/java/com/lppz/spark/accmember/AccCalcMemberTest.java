package com.lppz.spark.accmember;

import org.apache.spark.SparkContext;

import com.lppz.spark.accmember.bean.AccExportBean;
import com.lppz.spark.accmember.bean.TableBean;
import com.lppz.spark.accmember.scala.AccCalcMemberScoreHandler;
import com.lppz.spark.scala.jdbc.MysqlSpark;


/**
 * @author huangcheng
 */
class AccCalcMemberTest {
  
  public static void main(String[] args) {
	    AccCalcMemberScoreHandler  accCalcMemberScoreHandler = new AccCalcMemberScoreHandler();
    
        SparkContext sc =new MysqlSpark().buildSc("lppzAccCalcMemberScoreHandler", "local[4]");
        AccExportBean bean =new AccExportBean();

        bean.setPartition(100);
		bean.setSourceDriver("com.mysql.jdbc.Driver");
		bean.setSourceJdbcUrl("jdbc:mysql://10.8.202.231:3311/acc_export?useConfigs=maxPerformance&characterEncoding=utf8");
		bean.setSourcePwd("lppzacc");
		bean.setSourceUser("root");
		
		TableBean table=new TableBean();
		table.setBatchSize(300);
		table.setPartition(200);
		table.setPrimaryKey("_id");
		table.setSourceTableOrView("lp_memberscores_view");
    
		String dataSourcePath ="";
	  //Long lowerBound =8796093066201l //min的主键值
		//Long upperBound =9222427388889l //max的主键值
 
		Long lowerBound =1l;//第一条数据的索引
	    Long upperBound =47048800l;//最后一条数据的索引
	                    
	    Long pageSize=5000000l;
	    Long shang=upperBound/pageSize;
	    Long yushu=upperBound%pageSize;
	    if(yushu>0) shang = shang+1;//若余数大于0则循环次数加1，最后一次循环size大小为余数
	    System.out.println("47048800条数据，每次循环取5000000条，则循环次数为："+shang);
	    System.out.println("47048800条数据，每次循环取5000000条，则最后一次循环取值为："+yushu);
	    
	    Boolean  batch =true;
	    Long lowerBound1 =1l;
	    Long size =pageSize -1 ;
	    for(int i=1;i<=shang;i++){
	    	  Long upperBound1 =0l;
	        if(i==shang){//最后一次循环，不足500万，则取值为余数
	          upperBound1 =lowerBound1+yushu-1;
	        }else{//非最后一次循环，每次取固定的500万条数据
	        	upperBound1 =lowerBound1+size;
	        }
	        System.out.println("第"+(i)+"次，lowerBound:"+lowerBound1.toString()+"===upperBound:"+upperBound1.toString());
//	        accCalcMemberScoreHandler.startExportToRedisCuster(sc, bean, table, dataSourcePath, lowerBound1, upperBound1, batch);
	        lowerBound1 =upperBound1+1;
	    }
    
    
  }
  
  
}