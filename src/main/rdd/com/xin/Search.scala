package com.xin

import org.apache.spark.rdd.RDD

class Search(query: String) extends Serializable {

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatche1 (rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)   //等同于rdd.filter(x => x.contains(query))
  }

  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }
}
