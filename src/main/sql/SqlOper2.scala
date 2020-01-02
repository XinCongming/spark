class SqlOper2 {
}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object SqlOper2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SparkSQLStructTypeDemo").setMaster("local"))
    val sqlcontext = new SQLContext(sc)

    val lineRDD =  sc.textFile("sql\\sql1.txt").map(_.split(" "))
    //创建StructType对象  封装了数据结构(类似于表的结构)
    val structType: StructType = StructType {
      List(
        //列名   数据类型 是否为空(false是非空，即not nullable)
        StructField("id", IntegerType, false),
        StructField("name", StringType, true),
        StructField("age", IntegerType, false)
      )
    }
    //Row是spark.sql自带的，封装数据
    val rowRDD: RDD[Row] = lineRDD.map(arr => Row(arr(0).toInt,arr(1),arr(2).toInt))
    //将RDD转换为DataFrame
    val personDF: DataFrame = sqlcontext.createDataFrame(rowRDD,structType)
    personDF.show()
    println(personDF.orderBy("age").count())
  }
}

/**
 * 把数据写到mysql中
 */
import java.util.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object DataFormeInputJDBC {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("DataFormeInputJDBC").setMaster("local"))
    val sqlContext = new SQLContext(sc)
    val lines = sc.textFile("sql").map(_.split(" "))
    // StructType 存的表结构
    val structType: StructType = StructType(
      Array(
        StructField("id", IntegerType, false),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    //开始映射
    val rowRDD: RDD[Row] = lines.map(arr => Row(arr(0).toInt,arr(1),arr(2).toInt))
    //将当前RDD转换为DataFrame
    val personDF: DataFrame = sqlContext.createDataFrame(rowRDD,structType)

    //创建一个用于写入mysql配置信息
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","zxc123456")
    prop.put("driver","com.mysql.cj.jdbc.Driver")
    val jdbcurl = "jdbc:mysql://localhost/sparksql?characterEncoding=utf-8&serverTimezone=UTC"
    val table = "person"
    //propertities的实现是HashTable    表不存在会自动创建，存在会追加数据
    personDF.write.mode("append").jdbc(jdbcurl,table,prop)
    println("插入数据成功")
    sc.stop()
  }
}
