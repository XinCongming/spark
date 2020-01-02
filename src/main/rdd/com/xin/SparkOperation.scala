package com.xin

import org.apache.spark.{SparkConf, SparkContext}

class SparkOperation {

}

object SparkOperation{
  /**
   * 创建RDD
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //1、从内存创建
    val source = sc.parallelize((1 to 10),2)
    val source2 = sc.makeRDD(Array(1, 2, 3, 4))
    val source3 = sc.makeRDD(List(1, 2, 3, 4), 3)

    //2、外部创建
    val value = sc.textFile("in",3)   //3 文件是最小分区数
    val res = value.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
    
//    source.saveAsTextFile("out")
    res.saveAsTextFile("out2")

  }
}