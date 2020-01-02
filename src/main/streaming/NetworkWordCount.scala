class demo1 {
}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))   //每隔十秒刷新采集一次

    /*创建文本输入流,并进行词频统计 reduceByKey 结果不累加*/
    val lines = ssc.socketTextStream("hdp-1", 9999)
    lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).print()

    /*启动服务*/
    ssc.start()
    /*等待服务结束*/
    ssc.awaitTermination()
  }
}

object NetworkUpdateStateWordCount {
  /**
   * String : 单词 hello
   * Seq[Int] ：单词在当前批次出现的次数
   * Option[Int] ： 历史结果
   */
  //    val i : Int = 11;
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
  }

  def main(args: Array[String]) {
    //    LoggerLevel.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkUpdateStateWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    //做checkpoint 写入共享存储中
    ssc.checkpoint("E:/hadoop/hdpdata/checkpoint")
    val lines = ssc.socketTextStream("hdp-1", 9999)
    //val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    //updateStateByKey结果可以累加但是需要传入一个自定义的累加函数：updateFunc
    val results = lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}





