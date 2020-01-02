package com.xin

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//class SearchTwo(query: String) extends java.io.Serializable{
class SearchTwo(query: String) {
  // 必须加extends //Serializable 因为类初始化在Driver端
  // 而实际在executor端需要调用类中的方法
  //因此涉及到了网络通信需要序列化
  def isMatch(s:String):Boolean={
    s.contains(query)
  }
  def getMatchRdd(rdd:RDD[String])={
    rdd.filter(isMatch)    //相当于x => x.contains(query
  }
  def getMatchlRdd2(rdd:RDD[String])={
    val query_ = this.query
    rdd.filter(x => x.contains(query_))
    //必须赋值给局部变量,x.contains(this.query)不管用
  }
}
object serializeable {

  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val conf = new SparkConf().setMaster("local[*]").setAppName("serializeable")
    val sc = new SparkContext(conf)
    //2.创建一个RDD
    val rdd:RDD[String] = sc.parallelize(Array("hadoop", "spark","hive"))
    //3.创建一个Search对象
    val search = new SearchTwo("h")
    //4.运用第一个过滤函数并打印结果
    search.getMatchlRdd2(rdd).collect().foreach(println)
  }
}
