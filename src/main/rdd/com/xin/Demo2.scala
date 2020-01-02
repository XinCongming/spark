package com.xin

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * 双value操作
 */
class Demo2 {
}

object Demo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val RDD1: RDD[Int] = sc.parallelize(1 to 7)
    val RDD2: RDD[Int] = sc.makeRDD(6 to 10)
    //并集 union 两个rdd数据全部合并    1234567678910
    RDD1.union(RDD2).collect().foreach(print)
    //差集  只保留rdd1中数据且rdd2没有的数据    41523
    RDD1.subtract(RDD2).collect().foreach(print)
    //交集   67
    RDD1.intersection(RDD2).collect().foreach(print)
    //笛卡尔积(尽量不使用)   (1,6)(1,7)(1,8)(1,9)(1,10)(2,6)(3,6)(2,7)(3,7)(2,8)(3,8)(2,9)(2,10)...
    RDD1.cartesian(RDD2).collect().foreach(print)
  }
}

object zip {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val RDD1: RDD[Int] = sc.parallelize((1 to 5), 3)
    val RDD2: RDD[String] = sc.parallelize(Array("a", "b", "c", "d", "e"), 3)
    RDD1.zip(RDD2).collect().foreach(print) //(1,a)(2,b)(3,c)(4,d)(5,e)
  }
}

object partition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)
    println(rdd.partitions.size) //4
    val unit: RDD[(Int, String)] = rdd.partitionBy(new org.apache.spark.HashPartitioner(5))
    println(unit.partitions.size) //5
  }
}

object groupbykey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array("one", "two", "two", "three", "three", "three"))
    rdd.map(x => (x, 1)).groupByKey().collect().foreach(println)
    /*  (two,CompactBuffer(1, 1))
        (one,CompactBuffer(1))
        (three,CompactBuffer(1, 1, 1)) */
    //将相同key的值聚合，第二个参数：reduce任务的个数   wcordcount
    rdd.map(x => (x, 1)).reduceByKey((x, y) => x + y, 3).collect().foreach(println)
  }
}


object aggregateByKey {
  //创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myspark2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    rdd.glom().collect().foreach(array => println(array.mkString(",")))
    //aggregateByKey()()第一个括号内使进行操作比较的初始值，第二个()内传2个func，分别是分区内，分区外操作
    rdd.aggregateByKey(0)(math.max(_, _), _ + _).collect().foreach(println)

    //foldbykey将相同key的value相加
    rdd.foldByKey(0)(_ + _).collect().foreach(print) //(b,3)(a,5)(c,18)
    rdd.foldByKey(5)(_ + _).collect().foreach(print) //(b,8)(a,10)(c,28)

    //combineByKey  根据key计算每种key的均值
    //1.将所有数据改((k1,v1),v2)嵌套元组 2.将 (v1,v2)所有v1相加,v2加一，v是累加器现在的值
    //3.将所有分区的v1 v2合并   注意：2.3方法都是(v,v)形式,前v和现v
    val combined: RDD[(String, (Int, Int))] = rdd.combineByKey((_, 1), (x: (Int, Int), v) => (x._1 + v, x._2 + 1)
      , (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    combined.collect().foreach(print) //(b,(3,1))(a,(5,2))(c,(18,3))
    combined.map({ case (key, value) => (key, value._1 / value._2.toDouble) })
      .collect().foreach(print) //(b,3.0)(a,2.5)(c,6.0)

    sc.stop()
  }
}

object join {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local[*]"))
    val rdd3: RDD[(String, Int)] = sc.makeRDD(List(("我们", 1), ("2", 1), ("曾经", 1)))
    val rdd4: RDD[(String, Int)] = sc.makeRDD(List(("哈哈", 1), ("我们", 999), ("曾经", 1)))
    //只有相同k会组成(K,(V,W))格式
    rdd3.join(rdd4).collect().foreach(print) //(曾经,(1,1))(我们,(1,999))
    //cogroup返回(K,(Iterable<V>,Iterable<W>))   所有k都返回，rdd3中独有，rdd4没有的键,会返回空值代替
    //(哈哈,(CompactBuffer(),CompactBuffer(1)))(曾经,(CompactBuffer(1),CompactBuffer(1)))
    rdd3.cogroup(rdd4).collect().foreach(print)
  }
}

object action{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local[*]"))
    val rdd = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))
    //x,y是前后元组
    val tuple: (String, Int) = rdd.reduce((x,y)=>{(x._1+y._1,x._2+y._2)})
    println(tuple)        //(caad,12)

    rdd.take(2).foreach(array=>print(array.toString()))   //(a,1)(a,3)

    val rdd2 = sc.makeRDD(Array(("我们",1),("x",3),("1",3),("a",5)))
    rdd2.takeOrdered(3).foreach(x=>print(x.toString()))  //(1,3)(a,5)(x,3)
  }
}

object aggregate{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local[*]"))
    val rdd = sc.parallelize(1 to 10, 2)
   //0+1+2+3+4+5=15 0+6+7+8+9+10=40   0-15-40=-25
    val i: Int = rdd.aggregate(0)(_+_,_-_)
    print(i)  //-55

    //aggregate简化版，seqop和combop一样操作
    //0-1-2-3-4-5=-15 0-6-7-8-9-10=-40   0-(-15)-(-40)=55
    val i1: Int = rdd.fold(0)(_-_)
    print(i1)  //55

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("b",2),("c",8)))
    rdd2.countByKey().foreach(print)   //(a,2)(b,1)(c,1)
    rdd2.countByValue().foreach(print(_))  // ((c,8),1)((a,2),1)((a,1),1)((b,2),1)
  }
}

