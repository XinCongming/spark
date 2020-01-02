package atg

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object windows {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("23").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "hdp-1:2181", //zookeeper
      "dstream", //消费者组groupid
      Map("GMALL_STARTUP" -> 3) //map中存放多个topic主题，格式为：<topic,分区数>
    )
    //windows(windowLength, slideInterval)
    // 窗口大小，滑动步长(都是周期整数倍) 即3个周期数据，一次移动一个周期
    val windowed: DStream[(String, String)] = kafkaDStream.window(Seconds(9),Seconds(3))
    windowed.flatMap(t=>(t._2.split(" "))).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}

