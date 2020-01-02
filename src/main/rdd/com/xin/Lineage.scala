package com.xin

import org.apache.spark.{Dependency, SparkConf, SparkContext}

object Lineage {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("myspark2").setMaster("local[*]"))
    val wordAndOne = sc.textFile("in2/test.txt").flatMap(_.split(",")).map((_,1))
    val wordAndCount = wordAndOne.reduceByKey(_+_)

    val debugString: String = wordAndOne.toDebugString               //查看“wordAndOne”的Lineage
    val toDebugString: String = wordAndCount.toDebugString
    val dependencies: Seq[Dependency[_]] = wordAndOne.dependencies
    val dependencies1: Seq[Dependency[_]] = wordAndCount.dependencies

    println("----------------wordAndOne toDebugString------------------")
    println(debugString)     //HadoopRDD[0] at textFile---MapPartitionsRDD[1] at textFile--flatMap--map
    println("----------------wordAndCount toDebugString------------------")
    println(toDebugString)  //HadoopRDD[0] at textFile---MapPartitionsRDD[1] at textFile--flatMap--map--ShuffledRDD[4] at reduceByKey
    println("----------------wordAndOne dependencies------------------")
    println(dependencies.toString())   //org.apache.spark.OneToOneDependency@79e66b2f
    println("----------------wordAndCount dependencies------------------")
    println(dependencies1.toBuffer)    //org.apache.spark.ShuffleDependency@17273273
  }
}
