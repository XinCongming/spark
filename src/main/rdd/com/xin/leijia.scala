package com.xin
import org.apache.spark.{SparkConf, SparkContext}

//TODO 针对一个输入的日志文件，如果我们想计算文件中所有空行的数量，我们可以编写以下程序：
object leijia {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("HBaseRDD"))
    val notice = sc.textFile("in2/test.txt")
//  创建出存有初始值0的累加器
    val blanklines = sc.accumulator(0)
    val tmp = notice.flatMap(line => {
      if (line == "") {
        blanklines += 1
      }
      line.split(",")
    })

    val count: Long = tmp.count()
    println("单词个数："+count)
    val num: Int = blanklines.value
    println("空行个数："+num)    //如果文本最后一行是空值，不进行计算
  }
}

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

class LogAccumulator extends org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]] {
  private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()

  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  override def reset(): Unit = {
    _logArray.clear()
  }

  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  override def merge(other: org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]]): Unit = {
    other match {
      case o: LogAccumulator => _logArray.addAll(o.value)
    }

  }

  override def value: java.util.Set[String] = {
    java.util.Collections.unmodifiableSet(_logArray)
  }

  override def copy():org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]] = {
    val newAcc = new LogAccumulator()
    _logArray.synchronized{
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }
}

// 过滤掉带字母的
object LogAccumulator {
  def main(args: Array[String]) {
    val sc=new SparkContext(new SparkConf().setAppName("LogAccumulator").setMaster("local[3]"))

    val accum = new LogAccumulator    //new自定义的累加器
    sc.register(accum, "logAccum")    //通过sc注册累加器
    val sum: Int = sc.parallelize(Array("1", "2a", "3", "4b", "5", "xin", "7cd", "8", "9"), 2).filter(line => {
      val pattern = """^-?(\d+)"""    //^:行开头  (.\d+)?：括号里内出现0或1次   表数字
      val flag: Boolean = line.matches(pattern)   //匹配是否符合pattern
      if (!flag) {    //!flag -->包含字母
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum: " + sum)
    for (v <- accum.value) print(v + " ")
    println()
    sc.stop()
  }
}

object guangbo{
  def main(args: Array[String]): Unit = {
    val sc=new SparkContext(new SparkConf().setAppName("LogAccumulator").setMaster("local[2]"))
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    print(broadcastVar.value.mkString(" "))
  }
}


