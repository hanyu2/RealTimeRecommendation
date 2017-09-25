package me.hanyu.dataclean

import me.hanyu.conf.AppConf

object PopularMovies extends AppConf{
    val pop = hc.sql("select count(*) as c ,movieId from trainingData group by movieId order by c desc")
    import sqlContext.implicits._
    val pop5 = pop.select("movieId").map(x=>x.getInt(0)).take(5)
    for (i <- pop5) {
        val moviename = hc.sql(s"select title from movies where movieId=$i").first().getString(0)
        println(moviename)
    }
}