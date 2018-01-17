package com.lppz.spark.scala.test

object HelloWorld {
  var path:String="F:\\workspace\\Spark-History-Order\\src\\main\\resources\\delsql\\omsedb\\2015-08\\busi_lack_order.sql"
  
  def main(args: Array[String]): Unit = {
    println("Hello World")

    val subArray:Array[String]=new Array[String](2)
    
    val b = subArray.forall({x:String => x>""})
    
    println(b)
  }
}