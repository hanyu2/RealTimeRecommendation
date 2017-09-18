import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DataClearn {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile= sc.textFile("data/ratings.csv")
    println(textFile.count())
  }
}