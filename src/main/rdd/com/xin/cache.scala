package com.xin

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object cache {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("myspark2").setMaster("local[*]"))
    val LineRDD: RDD[String] = sc.makeRDD(Array("xin"))
    val mapRDD: RDD[String] = LineRDD.map(_+System.currentTimeMillis())
    val cacheRDD: RDD[String] = LineRDD.map(_+System.currentTimeMillis()).cache()  //做缓存
    for( a <- 1 to 5){
      mapRDD.foreach(println)             //五次输出时间戳不同
    }
    for( a <- 1 to 5){
      if(a==1)println("------------------")
      cacheRDD.foreach(println)          // 五次输出结果一致
    }
  }
}


//object partition{
//  def main(args: Array[String]): Unit = {
//    //创建一个pairRDD
//    val sc = new SparkContext(new SparkConf().setAppName("partition").setMaster("local[*]"))
//    val pairs = sc.parallelize(List((1,1),(2,2),(3,3)))
//    //查看RDD的分区器
//    val partitioner: Option[Partitioner] = pairs.partitioner
//
//    //导入HashPartitioner类
//    import org.apache.spark.HashPartitioner
//    //使用HashPartitioner对RDD进行重新分区
//    val partitioned = pairs.partitionBy(new HashPartitioner(2))
//    //查看重新分区后RDD的分区器
//    val partitioner2: Option[Partitioner] = partitioned.partitioner
//    }
//}
