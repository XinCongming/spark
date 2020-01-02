package atg

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object FileStream {

  def main(args: Array[String]): Unit = {

    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.监控文件夹创建DStream
    val dirStream: DStream[String] = ssc.textFileStream("hdfs://hdp-1:9000/spark") //不支持嵌套

    //4.将每一行数据做切分，形成一个个单词
    val wordStreams: DStream[String] = dirStream.flatMap(_.split("\t"))

    //5.将单词映射成元组（word,1）
    val wordAndOneStreams = wordStreams.map((_, 1))

    //6.将相同的单词次数做统计
    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_ + _)

    //7.打印
//    wordAndCountStreams.foreachRDD(x=>println(x))
    wordAndCountStreams.print()

    //8.启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
