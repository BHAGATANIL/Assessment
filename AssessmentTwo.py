import sqlite3
import pyspark.sql.functions as f
import pandas as pd

db_path = 'C:/sqlite/movies.db'
conn = sqlite3.connect(db_path)

query = 'SELECT * from directors'
query_one = 'SELECT * from movies'
query_two= 'SELECT * from people'
query_three= 'SELECT * from ratings'
query_four= 'SELECT * from stars'


dire_pandas_df = pd.read_sql_query(query, conn)
mov_pandas_df = pd.read_sql_query(query_one, conn)
peo_pandas_df = pd.read_sql_query(query_two, conn)
rat_pandas_df = pd.read_sql_query(query_three, conn)
sta_pandas_df = pd.read_sql_query(query_four, conn)


from pyspark import SparkContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)

directors = sqlCtx.createDataFrame(dire_pandas_df)
movies = sqlCtx.createDataFrame(mov_pandas_df)
people = sqlCtx.createDataFrame(peo_pandas_df)
ratings = sqlCtx.createDataFrame(rat_pandas_df)
stars = sqlCtx.createDataFrame(sta_pandas_df)

#directors.show()
#movies.show()
#people.show()
#ratings.show()
#stars.show()



que_two = directors.join(ratings, directors.movie_id == ratings.movie_id)\
                   .join(people, directors.person_id == people.id)\
                   .select(people.id, people.name, ratings.movie_id, ratings.rating, ratings.votes)

result_one = que_two.groupBy(f.col("movie_id").alias("mov_id"))\
                    .agg(f.avg("rating").alias("avg_rating"), f.count("movie_id").alias("TotalNoOfMoviesDirected"), f.sum("votes").alias("sum_vote"))\
                    .where((f.col('sum_vote') >= 100) & (f.col('TotalNoOfMoviesDirected') >= 4))\
                    .select("mov_id", "avg_rating", "TotalNoOfMoviesDirected")


directors_peo = directors.join(people, directors.person_id == people.id)\
                         .select(directors.movie_id, people.id, people.name)


final_result = result_one.join(directors_peo, result_one.mov_id == directors_peo.movie_id)\
                         .select(f.col("id").alias("DirectorID "), f.col("name").alias("DirectorName "),\
                         f.col("avg_rating").alias("AverageRatingOfMoviesDirected "), f.col("TotalNoOfMoviesDirected"))\
                         .orderBy(f.col("avg_rating").desc())
#final_result.show(10)

#-------------------------------------------------------------------------------------


most_no_mov = movies.join(ratings, movies.id == ratings.movie_id)\
                    .groupBy("year")\
                    .agg(f.count("title").alias("Most Number of Movies_pre"))\
                    .orderBy(f.col("Most Number of Movies_pre").desc())\
                    .limit(1)\
                    .select("Most Number of Movies_pre", "year")\
                    .withColumn("Most Number of Movies_pre", f.lit("Most Number of Movies"))

Highest_Rated_Year = movies.join(ratings, movies.id == ratings.movie_id)\
                           .groupBy("year")\
                           .agg(f.avg("rating").alias("Most Number of Movies_pre"))\
                           .orderBy(f.col("Most Number of Movies_pre").desc())\
                           .limit(1)\
                           .select("Most Number of Movies_pre", "year")\
                           .withColumn("Most Number of Movies_pre", f.lit("Highest Rated Year"))


least_no_mov = movies.join(ratings, movies.id == ratings.movie_id)\
                     .groupBy("year")\
                     .agg(f.count("title").alias("Least Number of Movies_pre"))\
                     .orderBy(f.col("Least Number of Movies_pre").asc())\
                     .limit(1)\
                     .select("Least Number of Movies_pre", "year")\
                     .withColumn("Least Number of Movies_pre", f.lit("Least Number of Movies"))


Lowest_Rated_Year = movies.join(ratings, movies.id == ratings.movie_id)\
                          .groupBy("year")\
                          .agg(f.avg("rating").alias("Lowest Rated Year_pre"))\
                          .orderBy(f.col("Lowest Rated Year_pre").asc())\
                          .limit(1)\
                          .select("Lowest Rated Year_pre", "year")\
                          .withColumn("Lowest Rated Year_pre", f.lit("Lowest Rated Year"))


Final_result = most_no_mov.union(Highest_Rated_Year)\
                          .union(least_no_mov)\
                          .union(Lowest_Rated_Year)\
                          .toDF("Category", "Year")

Final_result.show(truncate=False)