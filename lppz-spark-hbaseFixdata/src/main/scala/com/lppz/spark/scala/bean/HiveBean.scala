package com.lppz.spark.scala.bean

import scala.beans.BeanProperty

class HiveBean(@BeanProperty var hiveSchema:String,@BeanProperty var hiveTableName:String) 
    extends Serializable{
  
  def this()=this(null,null)
}