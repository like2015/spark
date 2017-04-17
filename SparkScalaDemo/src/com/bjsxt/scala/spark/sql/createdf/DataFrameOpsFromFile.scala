package com.bjsxt.scala.spark.sql.createdf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DataFrameOpsFromFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
//    val df = sqlContext.read.json("hdfs://hadoop1:9000/input/people.json")
     val df = sqlContext.read.json("people.json")
     
    df.registerTempTable("people")
    sqlContext.sql("select * from people where age > 20").show()
    
    /*df.show()

    df.printSchema()
    
    df.select("name").show()
    
    df.select(df("name"), df("age")+10).show()
    
    //SELECT * FROM TABLE WHERE age > 10
    df.filter(df("age")>10).show()
    
    df.groupBy("age").count.show()*/
  }
}