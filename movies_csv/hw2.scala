// Databricks notebook source
//Step 8: load the dataset with the input file movies.csv
val df = spark.read.option("header", "true")  
  .option("inferSchema","true")
  .csv("/FileStore/tables/movies.csv")

// COMMAND ----------

//Step 9: display the uploaded database
df.show(false)
df.count() 
// res6: Long = 31394 

// COMMAND ----------

//Step 10: register the database movies_table
df.createOrReplaceTempView("movies_table")

// COMMAND ----------

//Step 11: SQL: number of movies produced in each year sorted by year in ascending order
spark.sql("""select year, count(*) as count from movies_table group by year order by year asc""").show

// COMMAND ----------

//Step 12: DataFrames: number of movies produced in each year sorted by year in ascending order
df.groupBy("year").count().orderBy("year").show

// COMMAND ----------

//Step 13: SQL: five top most actors who acted in most number of movies //verified with both show(5) and limit(5)
spark.sql("""select actor, count(actor) as number_of_movies from movies_table group by actor order by count(actor) desc limit 5""").show

// COMMAND ----------

//Step 14: DataFrames: five top most actors who acted in most number of movies
df.groupBy("actor").count().withColumnRenamed("count", "number_of_movies").orderBy($"count".desc).show(5)

// COMMAND ----------

//Step 15: DataFrames: title and year for every movie that Tom Hanks acted in, sroted by ascending order without displaying name
df.select("title", "year").filter($"actor" === "Hanks, Tom").orderBy($"year".asc).show

// COMMAND ----------

//Step 16: Extra-Credit SQL: number of movies acted by each actor every year sorted from highest to lowest
spark.sql("""select actor, year, count(year) as number_of_movies from movies_table group by year, actor order by number_of_movies desc""").show

// COMMAND ----------

//Step 16: Extra-Credit DataFrames: number of movies acted by each actor every year sorted from highest to lowest
df.groupBy("year","actor").count().withColumnRenamed("count", "number_of_movies").orderBy($"count".desc).show
