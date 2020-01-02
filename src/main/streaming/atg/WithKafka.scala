package atg

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis}

object WithKafka {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("23").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "hdp-1:2181", //zookeeper
      "dstream", //消费者组groupid
      Map("first" -> 3) //map中存放多个topic主题，格式为：<topic,分区数>
    )

    val value: DStream[String] = kafkaDStream.flatMap(tuple=>tuple._2.split(","))
    val reduceDStream: DStream[(String, Int)] = value.map((_, 1)).reduceByKey(_ + _)

    reduceDStream.print()
//    reduceDStream.foreachRDD(rdd=>rdd.foreachPartition(r=>{
//      var jedis: Jedis = null
//        try {
//          jedis = JedisPoolUtil.getConnection
//        jedis.auth("123456")
//        r.foreach(date => {
//          jedis.hincrBy("sparkstreaming", date._1, date._2)
//        })
//      }catch {
//        case ex: Exception =>
//          ex.printStackTrace()
//      } finally {
//        if (jedis != null) jedis.close()
//      }
//    }))

    ssc.start()
    ssc.awaitTermination()
  }
}