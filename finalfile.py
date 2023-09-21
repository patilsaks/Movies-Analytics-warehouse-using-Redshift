import findspark
findspark.init()
import pyspark
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType, IntegerType, FloatType, BooleanType, ArrayType as Array
import pyspark.sql.functions as F
from pyspark.sql.functions import dayofmonth, month, year, quarter, expr

import os





spark = SparkSession.builder.master("local[1]").config("spark.jars.packages", "com.amazon.redshift:redshift-jdbc42:2.1.0.16").appName("SparkByExamples.com").getOrCreate()




spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

session = boto3.session.Session(
    aws_access_key_id="aws-secrest-key", 
    aws_secret_access_key="access-keys"
)

s3 = session.resource("s3")
bucket = s3.Bucket('movies-rec-bucket')
obj = bucket.Object('moviedataset.zip')

with io.BytesIO(obj.get()["Body"].read()) as tf:

    # rewind the file
    tf.seek(0)

    # Read the file as a zipfile and process the members
    with zipfile.ZipFile(tf, mode='r') as zipf:
        ##for subfile in zipf.namelist():
            #print(subfile)
        zipf.extractall(path='./')

spark.read.option('header', True).csv('/Users/ameetaamonkar/Downloads/keywords.csv').createOrReplaceTempView('sparkexample')


#filename keywords

keywords=spark.read.option('header', True).csv('/Users/ameetaamonkar/Documents/keywords.csv').createOrReplaceTempView('keywords')

#filename moviesmetadata

spark.read.option('header', True).csv('/Users/ameetaamonkar/Documents/movies_metadata.csv').createOrReplaceTempView('movies_metadata')
movies_metadata=spark.read.option('header', True).csv('/Users/ameetaamonkar/Documents/movies_metadata.csv')

# filename credits
credit=spark.read.option('header', True).csv('/Users/ameetaamonkar/Documents/credits.csv').createOrReplaceTempView('credits')

# filename links_small
links_small=spark.read.option('header', True).csv('/Users/ameetaamonkar/Documents/links_small.csv').createOrReplaceTempView('links_small')

# filename links
links=spark.read.option('header', True).csv('/Users/ameetaamonkar/Documents/links.csv').createOrReplaceTempView('links')

#file name ratings_small
ratings1=spark.read.option('header', True).csv('/Users/ameetaamonkar/Documents/ratings_small.csv').createOrReplaceTempView('ratings_small')

# filename ratings
ratings2=spark.read.option('header', True).csv('/Users/ameetaamonkar/Documents/ratings.csv').createOrReplaceTempView('ratings')

# filename consumer_price_index
consumer_price_index=spark.read.option('header', True).csv('/Users/ameetaamonkar/fredgraph.csv').createOrReplaceTempView('consumer_price_indexs')




#sample query
#spark.sql('select adult,belongs_to_collection,genres from movies_metadata').show(10)
#genre_schema = Array(StructType([StructField("id", IntegerType()), StructField("name", StringType())]))

#movies_metadata.withColumn("genres",from_json(col("genres"),genre_schema)).createOrReplaceTempView('sampleView')
#spark.sql('Select genres from sampleView').show(5)

result = movies_metadata.withColumn("genres",from_json("genres", "array<struct<id:int,name:string>>")).selectExpr("belongs_to_collection","inline(genres)","adult", "budget", "original_language", "title", "popularity", "revenue", "vote_count", "vote_average", "release_date")
result = result.withColumnRenamed('id','genreId')
result = result.withColumnRenamed('name','genreCategory')
 
result = result.withColumn("belongs_to_collection",from_json("belongs_to_collection", "array<struct<id:int,name:string,poster_path:string,backdrop_path:string>>")).selectExpr("inline(belongs_to_collection)","genreId","genreCategory","adult", "budget", "original_language", "title", "popularity", "revenue", "vote_count", "vote_average","release_date")
result = result.withColumnRenamed('id','movieId')
result = result.withColumnRenamed('name','movieName')


#result.show(5)

result.createOrReplaceTempView("results")
# creating dimentional table for genre

genre_dim=spark.sql('select DISTINCT genreID, genreCategory from results')

genre_dim.createOrReplaceTempView("movie_genre_di")
spark.sql('select * from movie_genre_di').show(15)

# creating dimentional table for genre_movie

movie_genre_dim=spark.sql('SELECT DISTINCT g1.genreID, r1.movieId FROM results r1 JOIN movie_genre_di g1 ON r1.genreID = g1.genreID')

movie_genre_dim.createOrReplaceTempView("movie_genreDim")

spark.sql('select * from movie_genreDim').show(15)



# dimentional model for movie_ratings

r=spark.sql('select * from ratings_small union all select * from ratings')
r.createOrReplaceTempView("ratingsAll")
#spark.sql('select * from ratingsAll').show(5) mergig 2 different rating files into one

movie_rating_dim=spark.sql('SELECT movieID, userId, rating FROM ratingsAll')


movie_rating_dim.createOrReplaceTempView('movie_ratingDim')
spark.sql('select * from movie_ratingDim').show(15)

# dimention date
date_dim = movies_metadata.select(dayofmonth("release_date").alias("day"),month("release_date").alias("month"), quarter("release_date").alias("quarter"),year("release_date").alias("year"),"release_date")
date_dim.createOrReplaceTempView("dateDimention")
spark.sql('select * from dateDimention').show(15)

#dim consumer_price_index
consumer_pricex=spark.sql('select dateDimention.release_date as date, consumer_price_indexs.CUSR0000SS62031 as consumer_price_index FROM dateDimention JOIN consumer_price_indexs ON dateDimention.release_date = consumer_price_indexs.date')
consumer_pricex.createOrReplaceTempView("consumer_price_indexes")
spark.sql('select * from consumer_price_indexes').show(15)


#fact table movie
fact_movie=spark.sql('''select mg.movieId, d.release_date, m1.adult, m1.budget, m1.original_language, m1.title, m1.popularity, m1.revenue, m1.vote_count, m1.vote_average from results m1 JOIN
                         movie_genreDim mg ON m1.movieId=mg.movieId JOIN dateDimention d ON  d.release_date=m1.release_date ''')
fact_movie.createOrReplaceTempView("fact_movies")
spark.sql('select * from fact_movies').show(15)



movie_genre_dim.write \
         .format("jdbc")  \
         .option("url", "jdbc:redshift://movies.108932157001.us-east-1.redshift-serverless.amazonaws.com:5439/dev") \
         .option("dbtable", "movies.stage_movie_genredim") \
         .option("user", "admin") \
         .option("password", "Shailesh1235") \
         .option("driver", "com.amazon.redshift.jdbc42.Driver") \
         .mode("append") \
         .save()






