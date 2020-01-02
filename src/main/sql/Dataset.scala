//import org.apache.spark.{SparkConf, SparkContext}
//
//object Dataset {
//  def main(args: Array[String]): Unit = {
//    val sc  =new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local[*]"))
//
//    val caseClassDS = Seq(Person("Andy", 32)).toDS()
//  }
//}
//
//case class Person(name : String,id : Long)
