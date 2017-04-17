package com.bjsxt.java.spark.core.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.common.base.Optional;

import scala.Tuple2;

public class JoinOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JoinOperator")
				.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<Integer,String>(1, "xuruyun"),
				new Tuple2<Integer,String>(2, "liangyongqi"),
				new Tuple2<Integer,String>(3, "wangfei"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer,Integer>(1, 150),
				new Tuple2<Integer,Integer>(2, 100)
				 );
		
		JavaPairRDD<Integer,String> nameRDD = sc.parallelizePairs(nameList,3);
		JavaPairRDD<Integer,Integer> scoreRDD = sc.parallelizePairs(scoreList,2);
		
		
		
		JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> leftOuterJoin = nameRDD.leftOuterJoin(scoreRDD);
		List<Tuple2<Integer, Tuple2<String, Optional<Integer>>>> collect = leftOuterJoin.collect();
		for (Tuple2<Integer, Tuple2<String, Optional<Integer>>> tuple2 : collect) {
			 if(tuple2._2._2.isPresent()){
				 System.out.println("tuple2._2._2.get():" + tuple2._2._2.get());
			 }
		}
		/*JavaPairRDD<Integer, String> mapToPair = nameRDD.mapToPair(new PairFunction<Tuple2<Integer,String>, Integer, String>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<Integer, String> t) throws Exception {
				 
				return null;
			}
		});
		
		mapToPair.filter(new Function<Tuple2<Integer,String>, Boolean>() {
			
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<Integer, String> v1) throws Exception {
				Integer _1 = v1._1;
				return _1 != 1?true:false;
			}
		}).collect();*/
		
		 
		
		// JOIN关联两个RDD
		// JOIN操作会把相同的KEY的值放到一个集合里面去
		JavaPairRDD<Integer, Tuple2<String, Integer>> results = nameRDD.join(scoreRDD);
		
		System.out.println("results.partitions().size():"+results.partitions().size());
		
		results.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple)
					throws Exception {
				System.out.println(tuple._1 + " " + tuple._2._1 + " " + tuple._2._2);
			} 
		});
		
		sc.close();
	}
}
