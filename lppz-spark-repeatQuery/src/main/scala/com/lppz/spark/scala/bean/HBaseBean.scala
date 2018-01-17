package com.lppz.spark.scala.bean

import scala.beans.BeanProperty

class HBaseBean(@BeanProperty var hbaseQuorum:String,@BeanProperty var hbasePort:String,
    @BeanProperty var tablebname:String,
    @BeanProperty var cf:String,@BeanProperty var partionNum:Integer,
    @BeanProperty var isWal:Boolean) extends Serializable{
  def this()=this(null,null,null,null,null,false)
}