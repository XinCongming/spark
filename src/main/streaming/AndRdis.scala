class AndRdis {
}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object NetworkWordCountToRedis {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("NetworkWordCountToRedis").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    /*创建文本输入流,并进行词频统计*/
    val lines = ssc.socketTextStream("hdp-1", 9999)
    val pairs: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    /*保存数据到Redis*/
    pairs.foreachRDD { rdd =>        //foreachRDD遍历DStream里面的RDD
      rdd.foreachPartition { partitionOfRecords =>    //遍历每个分区
        var jedis: Jedis = null
        try {
          jedis = JedisPoolUtil.getConnection     //获得jedis对象
          jedis.auth("123456")
          //hash <key,map>  map<String,String>    hincrBy(hashName,mapkey,mapvalue) Redis Hincrby 命令用于为哈希表中的字段值加上指定增量值。
          partitionOfRecords.foreach(record => jedis.hincrBy("wordCount", record._1, record._2))
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
        } finally {
          if (jedis != null) jedis.close()
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}


