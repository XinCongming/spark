package atg

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
//    val sc = new SparkContext(sparkConf)
//    val value: RDD[String] = sc.textFile("in2")
//
//    //flatMap():将整个文件数据切分打散，返回RDD[String]，即每一个单词各自独立
//    val value1: RDD[(String, Int)] = value.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
//    value1.map(x=>(x._2,x._1)).sortByKey(false).collect().foreach(println)
      0 to 10 foreach println

//    value.flatMap(_.split(",")).map((_,1)).sortBy(_._2,false).collect().foreach(println)
    //map():将整个文件数据切分，返回RDD[Array[String]]，即返回一个所有单词组成的整体数组

//    unit1.collect().foreach(array=>println(array.mkString("-")))
  }
}

//object test2{
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
//    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
//
//    val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("hdp-1",9999)
////    socketStream.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print();
//    socketStream.flatMap(_.split(",")).map((_,1)).updateStateByKey()
//
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}