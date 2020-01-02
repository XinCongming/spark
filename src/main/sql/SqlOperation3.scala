import SqlOperation2.Data
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取元数据json,并使用sql语句完成以下功能
 */
object SqlOperation3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local[*]"))

    //创建SQLContext对象
    val sqlc = new SQLContext(sc)
    val df: DataFrame = sqlc.read.json("in/info.json")

    //使用Sql语法
    //注册临时表，这个表相当于存储在 SQLContext中所创建对象中
    df.registerTempTable("t_person")

    //1 查询所有数据，并去除重复的数据；
    val sql = "select distinct * from t_person"

    //2 查询所有数据，打印时去除id字段；
    val sql2 = "select id,name from t_person"

    //3 筛选出age>30的记录；
    val sql3 = "select * from t_person where age>30"

    //4 查询所有记录的name列，并为其取名为username
    val sql4 = "select name as username from t_person"

    //5 查询年龄age的平均值
    val sql5 = "select avg(age) from t_person"

    //查询
    val res = sqlc.sql(sql)
    res.show() //默认打印是20行
    // 固化数据,将数据写到文件中mode是以什么形式写  写成什么文件
    //    res.write.mode("append").json("out3")
    //除了这两种还可以csv模式,json模式
    //    res.write.mode("append").save("out4")
  }

  //case class,编译器自动为你创建class和它的伴生 object，并实现了apply方法让你不需要通过 new 来创建类实例
  case class Person(id: Int, name: String, age: Int)

}

object spark {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local[*]"))
    joinRdd(sc)

    def joinRdd(sc: SparkContext) {
      val name = Array(
        Tuple2(1, "spark"),
        Tuple2(2, "tachyon"),
        Tuple2(3, "hadoop")
      )
      val score = Array(
        Tuple2(1, 100),
        Tuple2(2, 90),
        Tuple2(3, 80)
      )
      val namerdd: RDD[(Int, String)] = sc.parallelize(name);
      val scorerdd: RDD[(Int, Int)] = sc.parallelize(score);
      //join：在类型为(K,V)和(K,W)的RDD上调用，返回相同key对应的元素对的(K,(V,W))的RDD
      val result: RDD[(Int, (String, Int))] = namerdd.join(scorerdd);
      result.collect.foreach(println)
      println(result.count())
      println(result.take(3).mkString(","))
      /*
          (1,(spark,100))
          (2,(tachyon,90))
          (3,(hadoop,80))
          --------------------------------------
          3
          --------------------------------------
          (1,(spark,100)),(2,(tachyon,90)),(3,(hadoop,80))
       */
    }
  }
}

/**
 * 将数据分组排序取top3
 */
object group_sort {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName(""))
    val lined: RDD[String] = sparkContext.textFile("in/group_sort")

    val maped: RDD[(String, String)] = lined.map(r => {
      val strings: Array[String] = r.split("   ")
      val key: String = strings(0)
      val value: String = strings(1)
      (key, value)
    })
    val sorted: RDD[(String, Iterable[String])] = maped.groupByKey().sortBy(_._2)

    val maped2: RDD[(String, List[String])] = sorted.map(x => {
      val key: String = x._1
      val strings: List[String] = x._2.toList.reverse.take(3)
      (key, strings)
    })
    maped2.foreach(println)
  }
}

/*
*请将student.txt加载到内存中生成一个DataFrame,并按“id:1,name:张三，score:87”的格式
* 打印出DataFrame的所有数据。
 */
object student {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName(""))
    val lined: RDD[String] = sparkContext.textFile("in/3.txt")

    val maped: RDD[(String, String, String)] = lined.map(x => {
      val strings: Array[String] = x.split("：")
      (strings(0), strings(1), strings(2))
    })

    val personRDD: RDD[person] = maped.map(tuple => {
      person(tuple._1.toInt, tuple._2, tuple._3.toInt)
    })
    val sqlc: SQLContext = new SQLContext(sparkContext)
    import sqlc.implicits._
    val personDF: DataFrame = personRDD.toDF()
    personDF.foreachPartition(rdd => {
      val row: List[Row] = rdd.toList
      row.foreach(x => {
        println("id:" + x(0) + ",name:" + x(1) + ",score:" + x(2))
      })
    })
  }
  case class person(id: Int, name: String, score: Int)
}


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * 监控文件夹，实现单词统计，结果保存到HDFS
 */
object FileStream {
  def main(args: Array[String]): Unit = {
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.监控文件夹创建DStream
    val dirStream: DStream[String] = ssc.textFileStream("file:///home/hadoop/temp/") //不支持嵌套

    //4.将每一行数据做切分，形成一个个单词
    val wordStreams: DStream[String] = dirStream.flatMap(_.split(" "))

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