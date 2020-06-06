// Databricks notebook source
//Step 2: Load movies.csv
val df1 = spark.read.option("header", "true")  
  .option("inferSchema","true")
  .csv("/FileStore/tables/movies.csv")

// COMMAND ----------

//Step 2: Create table movies_table
df1.createOrReplaceTempView("movies_table") 

// COMMAND ----------

//Step 2: Load movie_ratings.csv
val df2 = spark.read.option("header", "true")     
  .option("inferSchema","true")
  .csv("/FileStore/tables/movie_ratings.csv")

// COMMAND ----------

//Step 2: Create table movie_review_table
df2.createOrReplaceTempView("movie_reviews_table")

// COMMAND ----------

//Display the contents in movies_table
df1.show()

// COMMAND ----------

//Display the contents in movies_review_table
df2.show()

// COMMAND ----------

//Step 3: Find the number of distinct movies in the file movies.csv.
df1.select("title").distinct.count()

// COMMAND ----------

//Step 4: Find the titles of the movies that appear in the file movies.csv but do not have a rating in the file movie_ratings.csv. Remark: the answer could be empty
df1.join(df2, df1.col("title")===df2.col("title"), "left_anti").select("title").show

// COMMAND ----------

//Step 5: Find the number of movies that appear in the ratings file (i.e., movie_ratings.csv) but not in the movies file (i.e., movies.csv)
df2.join(df1, df1.col("title")===df2.col("title"), "left_anti").select("title").distinct.count

// COMMAND ----------

//Step 6: Find the total number of distinct movies that appear in either movies.csv, or movie_ratings.csv, or both.
(df1.select("title")).union(df2.select("title")).distinct.count

// COMMAND ----------

//Step 7: Find the title and year for movies that were remade. These movies appear more than once in the ratings file with the same title but different years. Sort the output by title.
val tmp1 = df2.groupBy($"title").count().filter($"count" > 1)
tmp1.join(df2, Seq("title")).select("title","year").orderBy("title").show

// COMMAND ----------

//Step 8: Find the rating for every movie that the actor "Branson, Richard" appeared in. Schema of the output should be (title, year, rating)
df1.join(df2, Seq("title", "year")).select("title", "year", "rating").filter($"actor" === "Branson, Richard").show

// COMMAND ----------

//Step 9: Find the highest-rated movie per year and include all the actors in that movie. The output should have only one movie per year, and it should contain four columns: year, movie title, rating, and a list of actor names. Sort the output by year.

val tmp2 = df2.groupBy("year").max("rating").orderBy("year")
tmp2.join(df1, Seq("year")).select("year","title","max(rating)","actor").
withColumnRenamed("max(rating)", "rating").
withColumnRenamed("title", "highest_rated_movie").
withColumnRenamed("actor", "list_of_actors").
orderBy("year").show

// COMMAND ----------

//Step 10: Determine which pair of actors worked together most. Working together is defined as appearing in the same movie. The output should have three columns: actor 1, actor 2, and count. The output should be sorted by the count in descending order.
import org.apache.spark.sql.functions._
val t1 = df1.withColumnRenamed("actor", "actor1").select("actor1","title")
val t2 = df1.withColumnRenamed("actor", "actor2").select("actor2","title")
val joinExp = t1("title") === t2("title")
val result = t1.join(t2,joinExp).drop(t1("title")).filter(t1("actor1") =!= t2("actor2")).groupBy("actor1","actor2").count().orderBy("actor1","actor2").orderBy($"count".desc)//sort(desc("count"))
result.show()

// COMMAND ----------

//Step 11 : (Extra Credit)
import org.apache.spark.sql.functions.{sum, count, avg, expr}
import org.apache.spark.sql.functions.countDistinct

//calculates the number of movies released (from movies.csv and matches it with movie_ratings.csv) from 1937 till 2000 and the average rating of those movies
val avg1 = df2.select("title", "year", "rating").filter($"year" <= 2000).orderBy("year")
avg1.join(df1, Seq("title")).selectExpr("count(title)","avg(rating)").withColumnRenamed("count(title)", "number_of_movies_before_2000").withColumnRenamed("avg(rating)","average_rating").show

//calculates the number of movies released (from movies.csv and matches it with movie_ratings.csv) from 2000 till 2013 and the average rating of those movies
val avg2 = df2.select("title", "year", "rating").filter($"year" >= 2001).orderBy("year")
avg2.join(df1, Seq("title")).selectExpr("count(title)","avg(rating)").withColumnRenamed("count(title)", "number_of_movies_after_2000").withColumnRenamed("avg(rating)","average_rating").show
