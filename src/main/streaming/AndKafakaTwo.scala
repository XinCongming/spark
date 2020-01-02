class AndKafakaTwo {

}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//TODO 从kafka中采集数据经过分析后保存到Redis中
object SparkStreaming09_KafkaToSparkToRedis {

  def main(args: Array[String]): Unit = {

    //使用SparkStreaming完成WordCount

    //Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
    //实时数据分析配置对象
    //采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf,Seconds(5))

    //从kafka中采集数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hdp-1:2181",
      "zpark",
      Map("dstream" -> 3)
    )
    //将我们采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = kafkaDStream.flatMap(t=>t._2.split(" "))

    //将数据进行结构的转换方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_,1))

    //将转换结构后的数据进行聚合处理
    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)


    /*保存数据到Redis*/
    wordToSumDStream.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        var jedis: Jedis = null
        try {
          jedis = JedisPoolUtil.getConnection
          jedis.auth("123456")
          partitionOfRecords.foreach(record => jedis.hincrBy("wordCount", record._1, record._2))
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
        } finally {
          if (jedis != null) jedis.close()
        }
      }
    }

    //不能停止采集程序，所以不要加stop

    //启动采集器
    streamingContext.start()
    //Drvier等待采集器的执行
    streamingContext.awaitTermination()

  }

}
