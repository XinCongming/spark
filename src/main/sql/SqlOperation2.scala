import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SqlOperation2 {
  def main(args: Array[String]): Unit = {
    val sc  =new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local[*]"))

    //创建SQLContext对象
    val sqlc: SQLContext = new SQLContext(sc)
    val lineRDD: RDD[Array[String]] = sc.textFile("E:\\logs\\active.txt").map(_.split(","))
    //将获取数据 关联到样例类中  Person自定义的类
    val personRDD: RDD[Data] = lineRDD.map(x => Data(x(0),x(1).toInt))

    //toDF相当于反射，这里若要使用的话，需要导入包，且必须放在第一个toDF上面
    import sqlc.implicits._
    val personDF: DataFrame = personRDD.toDF()

    //    二、使用Sql语法
    //注册临时表，这个表相当于存储在 SQLContext中所创建对象中
    personDF.registerTempTable("t_data")
    val sql = "select id,count(time) num from t_data where time>20191205 group by id having num>7"
    //查询
    val res: DataFrame = sqlc.sql(sql)
    res.show()  //默认打印是20行
    res.write.mode("append").save("out10")
  }

  //case class,编译器自动为你创建class和它的伴生 object，并实现了apply方法让你不需要通过 new 来创建类实例
  case class Data(id:String,time:Long)
}


