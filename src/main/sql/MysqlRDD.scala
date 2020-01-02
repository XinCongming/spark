import java.sql.DriverManager
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
//TODO 读取mysql数据转换成RDD
object MysqlRDD {
  def main(args: Array[String]): Unit = {
    //2.创建SparkContext
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("JdbcRDD"))

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hdp-1:3306/sqoop"
    val userName = "root"
    val passWd = "123456"

    //创建JdbcRDD
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from tb3 where id >= ? and id <= ?;",
      1,     //id最小范围  对应第一个问号
      5,   //id最大范围   对应第二个问号
      1,   //分区数
      r => (r.getInt(1), r.getString(2))   //结果集处理
    )

    //打印最后结果
    println(rdd.count())
    rdd.foreach(println)

    sc.stop()
  }
}

//TODO 把RDD写入Mysql：
object writeMysql{
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("MysqlApp"))
    val data = sc.parallelize(List("Female", "Male","Female"))

    data.foreachPartition(insertData)
  }

  def insertData(iterator: Iterator[String]): Unit = {
    Class.forName ("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://hdp-1:3306/sqoop", "root", "123456")
    iterator.foreach(data => {
      val ps = conn.prepareStatement("insert into tb3(username) values (?)")
      ps.setString(1, data)
      ps.executeUpdate()
    })
  }
}


