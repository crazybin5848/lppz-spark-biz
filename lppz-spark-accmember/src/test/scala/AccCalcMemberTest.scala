import com.lppz.spark.accmember.scala.AccCalcMemberScoreHandler
import com.lppz.spark.scala.jdbc.MysqlSpark
import java.util.HashMap
import com.lppz.spark.accmember.bean.AccExportBean
import com.lppz.spark.accmember.bean.TableBean


/**
 * @author huangcheng
 */
object AccCalcMemberTest {
  
  def main(args: Array[String]): Unit = {
    
    var  accCalcMemberScoreHandler = new AccCalcMemberScoreHandler;
    
    var sc =new MysqlSpark().buildSc("lppzAccCalcMemberScoreHandler", "local[4]")
    sc.getConf.set("spark.sql.warehouse.dir", "/spark-warehouse/");

    var bean =new AccExportBean

    bean.setPartition(100);
		bean.setSourceDriver("com.mysql.jdbc.Driver");
		bean.setSourceJdbcUrl("jdbc:mysql://10.8.202.231:3311/acc_export?useConfigs=maxPerformance&characterEncoding=utf8");
				 //String jdbcurl = "jdbc:mysql://10.8.202.215:3311/acc_export?useConfigs=maxPerformance&characterEncoding=utf8";

		bean.setSourcePwd("lppzacc");
		bean.setSourceUser("root");
		
		var table=new TableBean;
		table.setBatchSize(300);
		table.setPartition(200);
		table.setPrimaryKey("_id");
		table.setSourceTableOrView("lp_memberscores_view");
    
		var dataSourcePath =""
	  //var lowerBound =8796093066201l //min的主键值
		//var upperBound =9222427388889l //max的主键值
 
		var lowerBound =1l//第一条数据的索引
    var upperBound =47048800l//最后一条数据的索引
                    
    var pageSize=5000000l
    var shang:Long=upperBound/pageSize
    var yushu:Long=upperBound%pageSize
    if(yushu>0) shang = shang+1//若余数大于0则循环次数加1，最后一次循环size大小为余数
    println("47048800条数据，每次循环取5000000条，则循环次数为："+shang)
    println("47048800条数据，每次循环取5000000条，则最后一次循环取值为："+yushu)
    
    var batch =true
    var lowerBound1:Long =1
    var size =pageSize -1 
    for(i<- 1l to shang){
    	  var upperBound1:Long =0l
        if(i==shang){//最后一次循环，不足500万，则取值为余数
          upperBound1 =lowerBound1+yushu-1
        }else{//非最后一次循环，每次取固定的500万条数据
        	upperBound1 =lowerBound1+size
        }
        println("第"+(i)+"次，lowerBound:"+lowerBound1.toString()+"===upperBound:"+upperBound1.toString())
//        accCalcMemberScoreHandler.startExportToRedisCuster(sc, bean, table, dataSourcePath, lowerBound1, upperBound1, batch)
        lowerBound1 =upperBound1+1
    }
    
    
  }
  
  
}