package me.hanyu.conf

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

trait AppConf {
  val localClusterURL = "local[2]"
  val clusterMasterURL = "spark://master:7077"
  val conf = new SparkConf().setMaster(clusterMasterURL)
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hc = new HiveContext(sc)

  //jdbc连接
  val jdbcURL = "jdbc:mysql://master:3306/hive_db"
  val recResultTable = "hive_db.user_movie_recommandation"
  val mysqlusername = "root"
  val mysqlpassword = "Training@1"
  val prop = new Properties
  prop.put("driver", "com.mysql.jdbc.Driver")
  prop.put("user", mysqlusername)
  prop.put("password", mysqlpassword)
}