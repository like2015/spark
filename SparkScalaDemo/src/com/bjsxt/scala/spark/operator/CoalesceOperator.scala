package com.bjsxt.scala.spark.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

object CoalesceOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CoalesceOperator").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val dataArr = Array("xuruyun1","xuruyun2","xuruyun3"
				,"xuruyun4","xuruyun5","xuruyun6"
				,"xuruyun7","xuruyun8","xuruyun9"
				,"xuruyun10","xuruyun11","xuruyun12")
		val dataRdd = sc.parallelize(dataArr, 2);
    
    val result = dataRdd.mapPartitionsWithIndex((index,x) => {
        val list = ListBuffer[String]()
        while (x.hasNext) {
          list+=("PartitionId"+index+"\tContent:"+x.next)
        }
        list.iterator
      })
      
      /**
       *  PartitionId0	Content:PartitionId0	Content:xuruyun1
          PartitionId0	Content:PartitionId0	Content:xuruyun2
          PartitionId0	Content:PartitionId0	Content:xuruyun3
          PartitionId0	Content:PartitionId0	Content:xuruyun4
          PartitionId0	Content:PartitionId0	Content:xuruyun5
          PartitionId0	Content:PartitionId0	Content:xuruyun6
          PartitionId1	Content:PartitionId1	Content:xuruyun7
          PartitionId1	Content:PartitionId1	Content:xuruyun8
          PartitionId1	Content:PartitionId1	Content:xuruyun9
          PartitionId1	Content:PartitionId1	Content:xuruyun10
          PartitionId1	Content:PartitionId1	Content:xuruyun11
          PartitionId1	Content:PartitionId1	Content:xuruyun12
       */
      
      
     val coalesceRdd = result.coalesce(4,true)
    
      val results = coalesceRdd.mapPartitionsWithIndex((index,x) => {
        val list = ListBuffer[String]()
        while (x.hasNext) {
          list+=("PartitionId"+index+"\tContent:"+x.next)
        }
        list.iterator
      })
      
     println(results.partitions.size)
     val resultArr = results.collect()
      for(a<-resultArr){
        println(a)
      }
    
    
    
  }
}