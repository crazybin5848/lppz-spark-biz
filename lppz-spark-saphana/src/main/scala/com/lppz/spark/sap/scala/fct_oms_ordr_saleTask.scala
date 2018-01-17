package com.lppz.spark.sap.scala;

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import java.util.Arrays
import org.apache.spark.sql.Row
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList
import org.apache.spark.sql.SQLContext
import java.util.Properties
import com.lppz.spark.scala.jdbc.MysqlSpark
import com.lppz.spark.scala.jdbc.SparkJdbcTemplete
import com.lppz.spark.sap.bean.Hive2DbmsBean
import com.lppz.spark.scala.HiveContextUtil
import com.lppz.spark.scala.SparkHdfsUtil
import com.lppz.spark.sap.handler.HaNaJdbcHandler
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import com.lppz.spark.util.SparkYamlUtils
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.TimSort

object fct_oms_ordr_saleTask extends Serializable{
  @transient lazy val LOG=Logger.getLogger(getClass.getName) 
  
  
  def main(args : Array[String])  = {
  
      val sc = new MysqlSpark().buildSc("fct_oms_ordr_saleTask", args(1))
      val bclass = new Hive2DbmsBean().getClass()
      val bean = SparkYamlUtils.loadYaml(args(0), false,bclass)
      
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val timestr = sdf.format(new Date().getTime).toString()
      //println(timestr)
      val calen1 = Calendar.getInstance()
      calen1.clear()
      calen1.set(Calendar.YEAR,2016)
      
      val yearFirstday = sdf.format(calen1.getTime())
      //println(yearFirstday)
      //HiveContextUtil.exec(sc, "use dwi")
      HiveContextUtil.exec(sc,"cache table oms_base_store_cache as select * from dwi.oms_base_store")
     
      HiveContextUtil.exec(sc,"cache table mater_cache as select * from utags.PZMATERIAL")
      HiveContextUtil.exec(sc,"cache table oms_prod_cache as select * from dwi.oms_prod")
      HiveContextUtil.exec(sc,"cache table dim_date_cache as select * from dim.dim_date")
      
      HiveContextUtil.exec(sc, "use "+bean.getHiveSchema)
      
      //跑取每个订单的会员编号   1028有赞商城店铺订单对应会员ID
      HiveContextUtil.exec(sc, "truncate table tmpparquet.tmp_fct_oms_ordr_sale3")
      
      val sql1 = "insert overwrite table tmpparquet.tmp_fct_oms_ordr_sale3                                                    "+ 
               "select t.zbpartner,                                                                                         "+
               "       unix_timestamp() as create_time,                                                                     "+
               "       o.ordr_code,                                                                                         "+
               "       o.date_id                                                                                            "+
               "from oms_ordr o                                                                                             "+
               "left join                                                                                                   "+
               "   (select s.store_no,                                                                                      "+
               "        s.store_code,                                                                                       "+
               "        a.zbsnumber,                                                                                        "+
               "        a.zbpartner                                                                                         "+
               "    from  azihyd0800 a                                                                                      "+
               "    inner join oms_base_store_cache s  on s.extrn_cust_type = a.zbstype                                       "+
               "    where   s.store_code='1028'                                                                             "+
               "         and (a.zbsnumber <> 'NULL' or length(a.zbsnumber) <> 0 or a.zbsnumber is not null )                     "+
               "         and a.zbstype like 'Z%') t                                                                         "+
               " on o.user_acct_name = regexp_replace(ltrim(regexp_replace(trim(t.zbsnumber),'0',' ')),' ','0') and o.store_no = t.store_no    "+
               " where t.store_code='1028'                                                                                   "+
               "   and (o.user_acct_name <> 'NULL' or length(o.user_acct_name) <> 0 or o.user_acct_name is not null )            "
              

        HiveContextUtil.exec(sc,sql1 + "and o.ds < %s".format(timestr))
        
        val sql2 ="insert into tmpparquet.tmp_fct_oms_ordr_sale3                                                          "+
               "select t.zbpartner,                                                                                    "+
               "       unix_timestamp() as create_time,                                                                "+
               "       o.ordr_code,                                                                                    "+
               "       o.date_id                                                                                       "+
               "from parquetustag.oms_ordr o                                                                           "+
               "left join                                                                                              "+
               "    (select s.store_no,                                                                                "+
               "            s.store_code,                                                                              "+
               "            a.zbsnumber,                                                                               "+
               "            a.zbpartner                                                                                "+
               "     from parquetustag.azihyd0800 a                                                                    "+
               "     inner join oms_base_store_cache s on s.extrn_cust_type=a.zbstype                                  "+
               "     where    s.store_code='1028'                                                                      "+
               "           and (a.zbsnumber<>'NULL' or length(a.zbsnumber)<>0 or a.zbsnumber is not null )               "+
               "           and a.zbstype like 'Z%') t                                                                  "+
               "on concat('0000000000000000000000000000000000000000000000000',o.ordr_cntct_mbp)=t.zbsnumber            "+
               "and o.store_no=t.store_no                                                                              "+
               "where t.store_code='1028'                                                                              "
          
        HiveContextUtil.exec(sc,sql2 + "and o.ds < %s and (o.ordr_cntct_mbp<>'NULL' or length(o.ordr_cntct_mbp)<>0 or o.ordr_cntct_mbp is not null )".format(timestr))
                
        //所有店铺订单对应会员ID汇总
        HiveContextUtil.exec(sc, "truncate table tmpparquet.tmp_fct_oms_ordr_sale4")
        val sql3 ="insert overwrite table tmpparquet.tmp_fct_oms_ordr_sale4                                                      "+	  
               "select t.zbpartner,                                                                                    "+
               "       t.ordr_code,                                                                                    "+
               "       t.date_id                                                                                       "+
               "from (select t1.zbpartner,                                                                             "+
               "      t1.ordr_code,                                                                                    "+
               "      t1.date_id                                                                                       "+
               "      from (select zbpartner,                                                                          "+
               "                   ordr_code,                                                                          "+
               "                   date_id,                                                                            "+
               "                   min(create_time)                                                                    "+
               "            from tmpparquet.tmp_fct_oms_ordr_sale3                                                     "+
               "            group by zbpartner,ordr_code,date_id                                                       "+
               "            ) t1                                                                                       "+
               "      union all                                                                                        "+
               "      select t.zbpartner,o.ordr_code,o.date_id                                                         "+
               "      from parquetustag.oms_ordr o                                                                     "+
               "      left join                                                                                        "+
               "          (select s.store_no,s.store_code,a.zbsnumber,a.zbpartner                                      "+
               "           from parquetustag.azihyd0800 a                                                              "+
               "           inner join oms_base_store_cache s on s.extrn_cust_type=a.zbstype                            "+
               "           where    s.store_code<>'1028'                                                               "+
               "                and (a.zbsnumber<>'NULL' or length(a.zbsnumber)<>0 or a.zbsnumber is not NULL )          "+
               "                and a.zbstype like 'Z%'                                                                "+
               "           ) t                                                                                         "+
               "           on o.user_acct_name=t.zbsnumber and o.store_no=t.store_no                                   "+
               "          where t.store_code<>'1028'                                                                   "
                                                                                        
        HiveContextUtil.exec(sc,sql3 + "and o.ds < %s and (o.user_acct_name<>'NULL' or length(o.user_acct_name)<>0 or o.user_acct_name is not NULL ) ) t".format(timestr))
        
        
        HiveContextUtil.exec(sc, "truncate table tmpparquet.tmp_fct_oms_ordr_sale5")
        val sql4 ="insert overwrite table tmpparquet.tmp_fct_oms_ordr_sale5                                 "+
               "select t1.zbpartner,                                                                     "+
               "       t1.ordr_code,                                                                     "+
               "       t1.date_id                                                                        "+
               "from (select t.zbpartner,                                                                "+
               "             t.ordr_code,                                                                "+
               "             t.date_id,                                                                  "+
               "             row_number() OVER(PARTITION BY t.ordr_code ORDER BY t.zbpartner ) AS rn     "+
               "      from tmpparquet.tmp_fct_oms_ordr_sale4 t                                           "+
               "             )t1                                                                         "+
               "where t1.rn=1                                                                          "
        HiveContextUtil.exec(sc,sql4)
        
        //将订单头和订单行关联，并把会员编号写入订单
        
        HiveContextUtil.exec(sc,"cache table tmp_fct_oms_ordr_sale5_cache as select * from tmpparquet.tmp_fct_oms_ordr_sale5");
     
      HiveContextUtil.exec(sc, "truncate table tmpparquet.fct_oms_ordr_sale_spark")
        val sql5 ="insert overwrite table tmpparquet.fct_oms_ordr_sale_spark                                          "+
               "select t.ordr_pymt_time,                                                                     "+
               "       t.date_id,                                                                            "+
               "       t.ordr_code,                                                                          "+
               "       t.ordr_detl_code,                                                                     "+
               "       t.extrn_ordr_no,                                                                      "+
               "       t.ordr_type,                                                                          "+
               "       t.ordr_cntct_phone,                                                                   "+
               "       t.ordr_cntct_mbp,                                                                     "+
               "       t.user_acct_name,                                                                     "+
               "       t.ordr_prov_code,                                                                     "+
               "       t.ordr_city_code,                                                                     "+
               "       t.ordr_cnty_code,                                                                     "+
               "       t.rcvg_adr,                                                                           "+
               "       t.decom_ordr_flg,                                                                     "+
               "       t.lgstc_sttus,                                                                        "+
               "       t.rcvr_name,                                                                          "+
               "       t.ordr_srce,                                                                          "+
               "       t.ordr_pymt_type,                                                                     "+
               "       t.ordr_pymt_sttus,                                                                    "+
               "       t.ordr_sbmt_time,                                                                     "+
               "       t.ordr_sttus,                                                                         "+
               "       t.need_invc_flg,                                                                      "+
               "       t.pos_prod_code,                                                                      "+
               "       t.prod_sale_num,                                                                      "+
               "       t.prod_sale_amt,                                                                      "+
               "       t.prod_price,                                                                         "+
               "       t.prod_type,                                                                          "+
               "       t.comb_prod_code,                                                                     "+
               "       t.prmtn_code,                                                                         "+
               "       t.prmtn_gift_no,                                                                      "+
               "       s.org_code,                                                                           "+
               "       s.store_code,                                                                         "+
               "       s.store_name,                                                                         "+
               "       s.extrn_cust_type,                                                                    "+
               "       p.prod_name,                                                                          "+
               "       p.prod_code,                                                                          "+
               "       pz.zsplyd AS prod_lyd,                                                                "+
               "       pz.zwldl  AS matl_big_categ,                                                          "+
               "       pz.zwlzl  AS matl_mdm_categ,                                                          "+
               "       pz.zwlxl  AS matl_small_categ,                                                        "+
               "       pz.matltype AS matl_type,                                                             "+
               "       z.zbpartner AS mbr_bp_code,                                                           "+
               "       t.whse_code,                                                                          "+
               "       t.ds as ds                                                                            "+
               "from (select o.ordr_pymt_time,                                                               "+
               "             to_date(o.ordr_pymt_time) as date_id,                                           "+
               "             o.ordr_code,                                                                    "+
               "             od.ordr_detl_code,                                                              "+
               "             o.extrn_ordr_no,                                                                "+
               "             o.ordr_type,                                                                    "+
               "             o.ordr_cntct_phone,                                                             "+
               "             o.ordr_cntct_mbp,                                                               "+
               "             o.user_acct_name,                                                               "+
               "             o.ordr_prov_code,                                                               "+
               "             o.ordr_city_code,                                                               "+
               "             o.ordr_cnty_code,                                                               "+
               "             o.rcvg_adr,                                                                     "+
               "             o.decom_ordr_flg,                                                               "+
               "             o.lgstc_sttus,                                                                  "+
               "             o.rcvr_name,                                                                    "+
               "             o.ordr_srce,                                                                    "+
               "             o.ordr_pymt_type,                                                               "+
               "             o.ordr_pymt_sttus,                                                              "+
               "             o.ordr_sbmt_time,                                                               "+
               "             o.ordr_sttus,                                                                   "+
               "             o.need_invc_flg,                                                                "+
               "             od.pos_prod_code,                                                               "+
               "             od.prod_sale_num,                                                               "+
               "             od.prod_sale_amt,                                                               "+
               "             od.prod_price,                                                                  "+
               "             od.prod_type,                                                                   "+
               "             od.comb_prod_code,                                                              "+
               "             od.prmtn_code,                                                                  "+
               "             od.prmtn_gift_no,                                                               "+
               "             to_date(o.ordr_pymt_time) as ds,                                                "+
               "             o.store_no,                                                                     "+
               "             t1.whse_code                                                                    "+
               "       from  parquetustag.oms_ordr o                                                         "+
               "       left join parquetustag.oms_ordr_detl od on o.ordr_no=od.ordr_no                       "+
               "       left join parquetustag.oms_dlvrd t1 on o.ordr_no = t1.ordr_no                         "+
               "       where   o.ordr_sttus <>'INVALID'                                                      "+
               "           and o.lppz_shppg_flg ='0'                                                         "+
               "           and o.ds <'2016-01-01'                                                            "+
               "           and od.ds <'2016-01-01'                                                           "+
               "           and t1.ds <'2016-01-01') t                                                        "+
               "left join oms_base_store_cache s on t.store_no=s.store_no                                    "+
               "left join oms_prod_cache p on t.pos_prod_code=p.prod_id                                      "+
               "left join mater_cache pz                                                                     "+
               "on t.pos_prod_code=regexp_replace(ltrim(regexp_replace(trim(pz.zmaterial),'0',' ')),' ','0') "+
               "left join tmp_fct_oms_ordr_sale5_cache z on t.ordr_code=z.ordr_code                       "
        
               HiveContextUtil.exec(sc,sql5)
               
             
               // uncache table
               
               HiveContextUtil.exec(sc,"uncache table oms_base_store_cache")
               HiveContextUtil.exec(sc,"uncache table mater_cache")
               HiveContextUtil.exec(sc,"uncache table oms_prod_cache")
               HiveContextUtil.exec(sc,"uncache table tmp_fct_oms_ordr_sale5_cache")
               

  }
                  
}