class ScalaWordCount {

}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    //创建spark配置，设置应用程序名字
    //val conf = new SparkConf().setAppName("ScalaWordCount")
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[4]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    //指定以后从哪里读取数据创建RDD（弹性分布式数据集）
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).sortBy(_._2, false).saveAsTextFile(args(1))

    val lines: RDD[String] = sc.textFile(args(0))
    //切分压平 _代表一行内容
    val words: RDD[String] = lines.flatMap(_.split(","))
    //将单词和一组合，形成一个元组
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //按key进行聚合  _+_是1+1，2+1，3+1 .....
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
//    wordAndOne.reduce((x,y)=>{   //有问题
//        if(x._1.equals(y._1)){
//        }
//      (x._1, x._2 + y._2)
//    })
//    wordAndOne.reduceByKey((x:Int,y:Int)=>x+y)
    //排序   _._2 根据第二个数据排序 <hello,3>根据3排序，false表示降序，ascending=false
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //将结果保存到HDFS中
    sorted.saveAsTextFile(args(1))
    //释放资源
    sc.stop()

  }
}