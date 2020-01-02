package atg

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

//TODO updateStateByKey实现累加wordcount，从kafka获取
object leijia {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("23").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
//    ssc.checkpoint("check")
    ssc.sparkContext.setCheckpointDir("check")

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "hdp-1:2181", //zookeeper
      "dstream", //消费者组groupid
      Map("dstream" -> 3) //map中存放多个topic主题，格式为：<topic,分区数>
    )

    val value: DStream[String] = kafkaDStream.flatMap(tuple=>tuple._2.split(","))
    val maped: DStream[(String, Int)] = value.map((_, 1))
    val stateDStream: DStream[(String, Int)] = maped.updateStateByKey {
      case (seq, buffer) => {
        //seq是序列，即maped的value，即1 1 1 1 的序列，是旧的值，buffer是内存中的数据，getOrElse如果获取不到，默认是0，是本次周期采集的数据
        val sum: Int = buffer.getOrElse(0) + seq.sum
//        seq.foldLeft(0)(_ + _)  //0初始值, _+_ 累加
        Option(sum)   //Some(sum)   Option包括Some和None
      }
    }
    stateDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
