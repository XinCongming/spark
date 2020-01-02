package com.xin
import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * 单value操作
 */

class Demo1 {
}
object Demo1{
  def main(args: Array[String]): Unit = {
    //将1-10各元素*2
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //map算子
    val unit = sc.makeRDD(1 to 10)
    val unit2 = sc.makeRDD(Array(1,2,3,4))
    val res = unit.map(x=>x*2)    //x=>x*2是spark的计算
    val res2 = unit2.map(x=>x*2)

//    res.collect().foreach(println)
//    res2.collect().foreach(print)

    //mappartiton对每一个分区进行计算  map对每一个元素进行计算
    //mappartiton效率由于map算子，减少了发送执行器执行交互次数
    //mappartiton可能存在内存溢出（OOM）
    val res3 = unit.mapPartitions(datas => {
      datas.map(x=>x*2)   //这一行整体对应一个executer, x=>x*2属于scala的计算，不属于spark
    })
    res3.collect().foreach(println)
  }
}

object mapparindex{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val unit = sc.makeRDD(1 to 10)
//  mapPartitionsWithIndex在mapPartitions基础上加了分区号，{}是模式匹配
    val tupleRDD = unit.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号是：" + num))
      }
    }
    tupleRDD.collect().foreach(println)
  }
}

object flatmap{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val unit = sc.makeRDD(Array(List(1,2),List(3,4)))
    val value: RDD[Any] = unit.flatMap(x=>x)
    val value1: RDD[List[Any]] = unit.map(x=>x)

    //flatmap 将每一个元素拆开压平
    value.collect().foreach(println)   // 1 2 3 4
    value1.collect().foreach(println)  //List(1, 2)  List(3, 4)
  }
}

object glom{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val unit = sc.makeRDD(1 to 10)
    //  将一个分区的数据放在一个array
    val glomRDD: RDD[Array[Int]] = unit.glom()
    glomRDD.collect().foreach(array =>println(array.mkString(",")+" 每个分区最大值："+array.max))
  }
}

object groupby{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val unit = sc.makeRDD(1 to 10)
    //分组后得到一个对偶元组<k,v> k是分组的值，v是分组的数据集合
    val groupbyRDD: RDD[(Int, Iterable[Int])] = unit.groupBy(_%3)
    groupbyRDD.collect().foreach(println)
  }
}

object filter{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val unit = sc.makeRDD(Array("xincm","xinjn","wang","wx","xincongm"))

    unit.filter(_.contains("xin")).collect().foreach(println)
  }
}

object sample{
  //抽样
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val unit = sc.makeRDD(1 to 10)
//  参数：1、是否放回   2、打分（大于这个分获取）false:0-1，true:0-1表示不重复，1以上重复  3、种子生成器 类似random
    unit.sample(true,7,1).collect().foreach(println)
    unit.sample(false,0.5,1).collect().foreach(println)
  }
}

object distinct{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val unit = sc.makeRDD(Array(1,2,3,1,2,6,9,8,5,0));

    //去重输出，存在shuffle机制：打乱重组到其他分区，distinct(2)指定去重后的分区数量
    //没有shuffle的速度快，分区之间不用等待，相互独立
    unit.distinct().collect().foreach(println)
    unit.distinct(2).collect().foreach(println)
  }
}

object coalesce{
  //缩减分区（合并分区）,缩减后的分区必须小于之前分区数，否则分区数不变
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val unit = sc.makeRDD((1 to 10),4);
    System.out.println("前分区数："+unit.partitions.size)
    val coalUnit: RDD[Int] = unit.coalesce(3)
//    val coalUnit: RDD[Int] = unit.coalesce(3,shuffle = false)
    System.out.println("后分区数："+coalUnit.partitions.size)

    //repartiton  重新洗牌,分区数可以大于原分区数   有shuffle
    val repartitionUnit: RDD[Int] = unit.repartition(6)
    System.out.println("repartition分区数: "+repartitionUnit.partitions.size)
  }
}

object sortBy{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val unit = sc.makeRDD(List(5,6,4,0,3,9,4,1),2);
//  根据大小排序，默认升序
    val res: RDD[Int] = unit.sortBy(x=>x)
    res.collect().foreach(print)   //01344569
    unit.sortBy(x=>x%3).collect().foreach(print)  //60394415
  }
}

object str{
  def main(args: Array[String]): Unit = {
    val str = "http://spark.zkari/xin"
    val i: Int = str.lastIndexOf("/")
    val str1: String = str.substring(i+1,str.length)

    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("E:\\hadoop\\hdpdata\\sparkwc\\input")

    val value: RDD[String] = lines.map(x => {
      x.substring(x.lastIndexOf("/") + 1)
    })

    value.collect().foreach(println)
  }
}

object wcip{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("E:\\hadoop\\hdpdata\\sparkwc\\input")
    val value: RDD[String] = lines.map(x => {
      x.substring(x.lastIndexOf("/") + 1,x.length)
    })
    val res: Array[(String, Int)] = value.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).collect()
//    res.foreach(println)
    println(res.toBuffer)
    println(res.mkString(","))

    sc.stop()
  }
}

object groupTopN{
  def main(args: Array[String]): Unit = {
    val subject = Array("bigdata","javaee","php")
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("in")
    //切分数据，形成((job, person), 1)的(嵌套)元组
    val value: RDD[((String, String), Int)] = lines.map(x => {
      val i: Int = x.lastIndexOf("/")
      val person: String = x.substring(i + 1, x.length)
      val urlString: String = x.substring(0, i)
      val url = new URL(urlString)                                   //url带一个gethost获取域名
      val splits: Array[String] = url.getHost.split("\\.")   //转义
      val job: String = splits(0)
      ((job, person), 1)
    })

    val reduced: RDD[((String, String), Int)] = value.reduceByKey(_+_)  //_是1+1，2+1，3+1
/*    //方法一：groupby方式
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)
    //mapValues不改变key值，只对value进行操作

    //    val unit: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))
    val unit: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(x => {
      //将value转换成list，调用list的sortby排序，这里是scala的排序，不是spark的
      val list: List[((String, String), Int)] = x.toList
      val tuples: List[((String, String), Int)] = list.sortBy(_._2).reverse.take(3)
      tuples   //将数据传递下去
    })
    unit.collect().foreach(println)    */

    //方法二：for循环，filter   for循环三个职业，filter过滤实现分组
/*    for(sb <- subject){
      reduced.filter(_._1._1.equals(sb)).sortBy(_._2,false).collect().foreach(println)
    }       */

    //方法三：重写partitonby的方法，使每个职业单独一个分区，分区输出
    //sortby会打乱分区，所以先排序，后分区
    reduced.sortBy(_._2,false).partitionBy(new myPartition(3))
      .glom().collect().foreach(array=>println(array.mkString(",")))

//      .saveAsTextFile("out3")
  }
}

class myPartition(num :Int) extends Partitioner{
  override def numPartitions: Int = num
  override def getPartition(key: Any): Int = {
    var partValue : Int = key match {
      case key if key.toString.contains("bigdata") =>0
      case key if key.toString.contains("javaee") =>1
      case key if key.toString.contains("php") =>2
    }
    print("<"+key+","+partValue+">")
    partValue
  }
}

