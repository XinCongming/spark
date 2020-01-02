package atg

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class FileStreamTwo {

}

object FileStreamTwo{
  def main(args: Array[String]): Unit = {
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("StreamWordCount")
    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.创建自定义receiver的Streaming
    val lineStream = ssc.receiverStream(new CustomerReceiver("hdp-1", 9999))

    //4.将每一行数据做切分，形成一个个单词
    lineStream.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).print()
    //8.启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
