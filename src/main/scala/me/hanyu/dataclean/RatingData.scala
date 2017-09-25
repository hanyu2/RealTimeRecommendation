package me.hanyu.dataclean

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object RatingData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ETL").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hc = new HiveContext(sc)
    
    //divide rating data to training data and test data
    val ratings = hc.sql("cache table ratings")
    val count = hc.sql("select count(*) from ratings").first().getLong(0).toInt
    val percent = 0.6
    val trainingdatacount = (count * percent).toInt
    val testdatacount = (count * (1 - percent)).toInt
    
    val trainingDataAsc = hc.sql(s"select userId,movieId,rating from ratings order by timestamp asc")
    trainingDataAsc.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingDataAsc")
    hc.sql("drop table if exists trainingDataAsc")
    hc.sql("create table if not exists trainingDataAsc(userId int,movieId int,rating double) stored as parquet")
    hc.sql("load data inpath '/tmp/trainingDataAsc' overwrite into table trainingDataAsc")

    val trainingDataDesc = hc.sql(s"select userId,movieId,rating from ratings order by timestamp desc")
    trainingDataDesc.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingDataDesc")
    hc.sql("drop table if exists trainingDataDesc")
    hc.sql("create table if not exists trainingDataDesc(userId int,movieId int,ratings double) stored as parquet")
    hc.sql("load data inpath '/tmp/trainingDataDesc' overwrite into table trainingDataDesc")

    val trainingData = hc.sql(s"select * from trainingDataAsc limit $trainingdatacount")
    trainingData.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingData")
    hc.sql("drop table if exists trainingData")
    hc.sql("create table if not exists trainingData(userId int,movieId int,rating double) stored as parquet")
    hc.sql("load data inpath '/tmp/trainingData' overwrite into table trainingData")

    val testData = hc.sql(s"select * from trainingDataDesc limit $testdatacount")
    testData.write.mode(SaveMode.Overwrite).parquet("/tmp/testData")
    hc.sql("drop table if exists testData")
    hc.sql("create table if not exists testData(userId int,movieId int,rating double) stored as parquet")
    hc.sql("load data inpath '/tmp/testData' overwrite into table testData")
    
    val ratingRDD = hc.sql("select * from trainingData").rdd.map(x => Rating(x.getInt(0),x.getInt(1),x.getDouble(2)))
    
    val model = ALS.train(ratingRDD, 1, 10)
  }
}