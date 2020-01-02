import org.apache.spark.{SparkConf, SparkContext}

class SqlOperation {
}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SqlOperation {
  def main(args: Array[String]): Unit = {
    val sc  =new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local[*]"))

    //创建SQLContext对象
    val sqlc = new SQLContext(sc)
    val lineRDD: RDD[Array[String]] = sc.textFile("sql\\sql1.txt").map(_.split(" "))
    //将获取数据 关联到样例类中  Person自定义的类
    val personRDD: RDD[Person] = lineRDD.map(x => Person(x(0).toInt,x(1),x(2).toInt))

    //toDF相当于反射，这里若要使用的话，需要导入包，且必须放在第一个toDF上面
    import sqlc.implicits._
    val personDF: DataFrame = personRDD.toDF()
//    val personDF: DataFrame = personRDD.toDF("ID","NAME","AGE")
//    一、DSL风格
    personDF.show()
    personDF.select("id","name","age").filter("age>15").show()

//    二、使用Sql语法
    //注册临时表，这个表相当于存储在 SQLContext中所创建对象中
    personDF.registerTempTable("t_person")
    val sql = "select * from t_person where age > 20 order by age"
    //查询
    val res = sqlc.sql(sql)
    res.show()  //默认打印是20行
    // 固化数据,将数据写到文件中mode是以什么形式写  写成什么文件
    //    res.write.mode("append").json("out3")
    //除了这两种还可以csv模式,json模式
    res.write.mode("append").save("out4")
  }

  //case class,编译器自动为你创建class和它的伴生 object，并实现了apply方法让你不需要通过 new 来创建类实例
  case class Person(id:Int,name:String,age:Int)
}


