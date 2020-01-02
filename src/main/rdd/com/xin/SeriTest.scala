package com.xin

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SeriTest {

    def main(args: Array[String]): Unit = {

      //1.初始化配置信息及SparkContext
      val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
      val sc = new SparkContext(sparkConf)

      //2.创建一个RDD
      val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

      //3.创建一个Search对象
      val search = new Search("spa")

      //4.运用第一个过滤函数并打印结果
      val match1: RDD[String] = search.getMatche1(rdd)
      match1.collect().foreach(println)
    }
}
