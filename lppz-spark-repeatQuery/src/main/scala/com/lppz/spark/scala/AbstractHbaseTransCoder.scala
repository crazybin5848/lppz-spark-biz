package com.lppz.spark.scala

import scala.beans.BeanProperty

/**
 * @author zoubin
 */
abstract class AbstractHbaseTransCoder(@BeanProperty protected var transferCoder : ((java.util.HashMap[String,String],java.util.Map[String,String]) => java.util.HashMap[String,String])) {
  def buildParamMap(): java.util.Map[String,String]
  def this()=this(null)
}