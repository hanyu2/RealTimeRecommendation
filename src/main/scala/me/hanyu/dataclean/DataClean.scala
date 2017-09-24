package me.hanyu.dataclean

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode


import me.hanyu.caseclass.Links
import me.hanyu.caseclass.Ratings
import me.hanyu.caseclass.Movies
import org.apache.spark.sql.hive.HiveContext

object DataClean {
  def main(args: Array[String]): Unit = {
    //Note that we run with local[2], meaning two threads -
    //which represents “minimal” parallelism, 
    //which can help detect bugs that only exist when we run in a distributed context.
    val conf = new SparkConf().setAppName("ETL").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hc = new HiveContext(sc)
    import sqlContext.implicits._
    val links = sc.textFile("data/links.csv", 8)
    val ratings = sc.textFile("data/ratings.csv", 8)
    val movies = sc.textFile("data/movies.csv", 8)
    val tags = sc.textFile("data/tags.csv", 8)
    val linksRDD = deleteHeader(links)
    val ratingsRDD = deleteHeader(ratings)
    val moviesRDD = deleteHeader(movies)
    val cleanedLinks = linksRDD.map(_.split(",")).filter(_.length==3).map(x=>Links(x(0).trim.toInt, x(1).trim.toInt, x(2).trim().toInt)).toDF()
    val cleanedMovies = ratingsRDD.map(_.split(",")).filter(_.length==3).map(x => Ratings(x(0).trim().toInt, x(1).trim().toInt, x(2).trim().toDouble, x(3).trim().toInt)).toDF()
    val cleanedRatings = moviesRDD.map(_.split(",")).filter(_.length==4).map(x => Movies(x(0).trim().toInt, x(1).trim(), x(2).trim())).toDF()
    val cleanedTags = moviesRDD.map(_.split(",")).filter(_.length==4).map(x => Movies(x(0).trim().toInt, x(1).trim(), x(2).trim())).toDF()

    //save table to hive
    cleanedLinks.write.mode(SaveMode.Overwrite).parquet("/tmp/links")
    hc.sql("drop table if exists links")
    hc.sql("create table if not exists links(movieId int,imdbId int,tmdbId int) stored as parquet")
    hc.sql("load data inpath '/tmp/links' overwrite into table links")
    
    cleanedMovies.write.mode(SaveMode.Overwrite).parquet("/tmp/movies")
    hc.sql("drop table if exists movies")
    hc.sql("create table if not exists movies(movieId int,title string,genres string) stored as parquet")
    hc.sql("load data inpath '/tmp/movies' overwrite into table movies")
    
    cleanedRatings.write.mode(SaveMode.Overwrite).parquet("/tmp/ratings")
    hc.sql("drop table if exists ratings")
    hc.sql("create table if not exists ratings(userId int,movieId int,rating double,timestamp int) stored as parquet")
    hc.sql("load data inpath '/tmp/ratings' overwrite into table ratings")
    
    cleanedTags.write.mode(SaveMode.Overwrite).parquet("/tmp/tags")
    hc.sql("drop table if exists tags")
    hc.sql("create table if not exists tags(userId int,movieId int,tag string,timestamp int) stored as parquet")
    hc.sql("load data inpath '/tmp/tags' overwrite into table tags")
  }

  def deleteHeader(rdd: RDD[String]): RDD[String] = {
    rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
  }
}