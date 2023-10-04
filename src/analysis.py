import re
import findspark
import pyspark.sql.functions as f
from pyspark.sql.functions import col, when, year
import datetime
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, ArrayType


findspark.init()
from pyspark.sql import SparkSession

def init_spark(app_name: str):
  spark = SparkSession.builder.appName(app_name).getOrCreate()
  sc = spark.sparkContext
  return spark, sc

spark, sc = init_spark('demo')
sc

noneList = [None] * 26
#advanced read csv file#
"""
#advanced read
spark.read.option("Header", True)\
    .option("multiline", "true")\
    .option("escape", "\"")\
    .csv(usersPath)
"""
#------------------------------------------------------------
usersPath = "users.csv"
users_df = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(usersPath)

#users_df.printSchema()
#users_df.show()
#------------------------------------------------------------
ticketsPath = "tickets.csv"
tickets_df = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(ticketsPath)

#tickets_df.printSchema()
#tickets_df.show()
#------------------------------------------------------------
queriesPath = "queries.csv"
queries_df = spark.read.option("Header", True)\
    .option("multiline", "true")\
    .option("escape", "\"")\
    .csv(queriesPath)

#extracting date
extractDate = f.udf(lambda x: list(eval(x))[0] if x != '[]' else None)
queries_df = queries_df.withColumn("from_realese_date", extractDate(col("from_realese_date")))

#saving empty arrays (in order not to mistake them for none and lose their rows)
theFillerList = f.udf(lambda x: x if x != '[]' else "['']")

#the goal of this function is to convert string to list
#and then adding nones in order not to get out of array
#because each array has its own size
fixedListBuilder = f.udf(lambda x: list(eval(x)) + list(noneList))

#theFillerList used on each arrayType column
queries_df = queries_df.withColumn("genres", theFillerList(col("genres")))
queries_df = queries_df.withColumn("lang", theFillerList(col("lang")))
queries_df = queries_df.withColumn("actors", theFillerList(col("actors")))
queries_df = queries_df.withColumn("director", theFillerList(col("director")))
queries_df = queries_df.withColumn("cities", theFillerList(col("cities")))
queries_df = queries_df.withColumn("country", theFillerList(col("country")))
queries_df = queries_df.withColumn("production_company", theFillerList(col("production_company")))

#fixedListBuilder used on each arrayType column
queries_df = queries_df.withColumn("genres", fixedListBuilder(col("genres")))
queries_df = queries_df.withColumn("lang", fixedListBuilder(col("lang")))
queries_df = queries_df.withColumn("actors", fixedListBuilder(col("actors")))
queries_df = queries_df.withColumn("director", fixedListBuilder(col("director")))
queries_df = queries_df.withColumn("cities", fixedListBuilder(col("cities")))
queries_df = queries_df.withColumn("country", fixedListBuilder(col("country")))
queries_df = queries_df.withColumn("production_company", fixedListBuilder(col("production_company")))

#splitting each array into seperate columns
for i in range (5):
    itemExtract = f.udf(lambda x: x[i] if x[i] != None  else None)
    col1 = 'genre_{}'.format(i+1)
    col2 = 'language_{}'.format(i + 1)
    col3 = 'actor_{}'.format(i + 1)
    col4 = 'director_{}'.format(i + 1)
    col5 = 'city_{}'.format(i + 1)
    col6 = 'country_{}'.format(i + 1)
    col7 = 'production_company_{}'.format(i + 1)
    queries_df = queries_df.withColumn(col1, itemExtract(col("genres")))
    queries_df = queries_df.withColumn(col2, itemExtract(col("lang")))
    queries_df = queries_df.withColumn(col3, itemExtract(col("actors")))
    queries_df = queries_df.withColumn(col4, itemExtract(col("director")))
    queries_df = queries_df.withColumn(col5, itemExtract(col("cities")))
    queries_df = queries_df.withColumn(col6, itemExtract(col("country")))
    queries_df = queries_df.withColumn(col7, itemExtract(col("production_company")))

#collecting all seperate columns
queries_df = queries_df.withColumn("genresFinal", f.array('genre_1','genre_2','genre_3','genre_4','genre_5'))
queries_df = queries_df.withColumn("languageFinal", f.array('language_1','language_2','language_3','language_4','language_5'))
queries_df = queries_df.withColumn("actorsFinal", f.array('actor_1','actor_2','actor_3','actor_4','actor_5'))
queries_df = queries_df.withColumn("directorsFinal", f.array('director_1','director_2','director_3','director_4','director_5'))
queries_df = queries_df.withColumn("citiesFinal", f.array('city_1','city_2','city_3','city_4','city_5'))
queries_df = queries_df.withColumn("countriesFinal", f.array('country_1','country_2','country_3','country_4','country_5'))
queries_df = queries_df.withColumn("production_companiesFinal", f.array('production_company_1','production_company_2',
                                                                        'production_company_3','production_company_4','production_company_5'))
#drops
for i in range(5):
    col1 = 'genre_{}'.format(i + 1)
    col2 = 'language_{}'.format(i + 1)
    col3 = 'actor_{}'.format(i + 1)
    col4 = 'director_{}'.format(i + 1)
    col5 = 'city_{}'.format(i + 1)
    col6 = 'country_{}'.format(i + 1)
    col7 = 'production_company_{}'.format(i + 1)
    queries_df = queries_df.drop(col1, col2, col3, col4, col5, col6, col7)

queries_df = queries_df.drop('genres','lang', 'actors', 'director', 'cities', 'country', 'production_company')

yearExtractQueries = f.udf(lambda x: None if x == None else (datetime.datetime.strptime(x, "%Y").year if ('-' in x)
                                                         else datetime.datetime.strptime(x, "%Y").year))
#queries_df.printSchema()
#queries_df.show(5)
#------------------------------------------------------------
moviesPath = "movies.csv"
movies_df = spark.read.option("Header", True)\
    .option("multiline", "true")\
    .option("escape", "\"")\
    .csv(moviesPath)

#year extraction
yearExtractMovies = f.udf(lambda x: None if x == None else (datetime.datetime.strptime(x, "%Y-%m-%d").year if ('-' in x)
                                                         else datetime.datetime.strptime(x, "%d/%m/%Y").year))

movies_df = movies_df.withColumn('year', yearExtractMovies("release_date"))

originalDF = movies_df
#functions to save empty array(in order not to mistake them for none and lose their rows)
theFillerGenres = f.udf(lambda x: x if x != '[]' else "[{'id':'','name':''}]")
thefillerCompanies = f.udf(lambda x: x if x != '[]' else "[{'name':'','id':''}]")
theFillerCountries = f.udf(lambda x: x if x != '[]' else "[{'iso_3166_1':'','name':''}]")
theFillerLanguages = f.udf(lambda x: x if x != '[]' else "[{'iso_639_1':'','name':''}]")

#theFillerGenres used on each arrayType column
movies_df = movies_df.withColumn("genres", theFillerGenres(col("genres")))
movies_df = movies_df.withColumn("production_companies", thefillerCompanies(col("production_companies")))
movies_df = movies_df.withColumn("production_countries", theFillerCountries(col("production_countries")))
movies_df = movies_df.withColumn("spoken_languages", theFillerLanguages(col("spoken_languages")))
movies_df = movies_df.withColumn("cities", theFillerList(col("cities")))

#fixedListBuilder used on each arrayType column
movies_df = movies_df.withColumn("genres", fixedListBuilder(col("genres")))
movies_df = movies_df.withColumn("production_companies", fixedListBuilder(col("production_companies")))
movies_df = movies_df.withColumn("production_countries", fixedListBuilder(col("production_countries")))
movies_df = movies_df.withColumn("spoken_languages", fixedListBuilder(col("spoken_languages")))
movies_df = movies_df.withColumn("cities", fixedListBuilder(col("cities")))

#splitting array which contains dics to seperate columns
for i in range (26):
    idExtract = f.udf(lambda x: x[i]['id'] if x[i] != None  else None)
    nameExtract = f.udf(lambda x: x[i]['name'] if x[i] != None  else None)
    iso3166Extract = f.udf(lambda x: x[i]['iso_3166_1'] if x[i] != None else None)
    iso639Extract = f.udf(lambda x: x[i]['iso_639_1'] if x[i] != None else None)
    col1 = 'genre_{}_id'.format(i+1)
    col2 = 'genre_{}_name'.format(i+1)
    col3 = 'production_company_{}_name'.format(i + 1)
    col4 = 'production_company_{}_id'.format(i + 1)
    col5 = 'production_country_{}_iso'.format(i + 1)
    col6 = 'production_country_{}_name'.format(i + 1)
    col7 = 'language_{}_iso'.format(i + 1)
    col8 = 'language_{}_name'.format(i + 1)
    movies_df = movies_df.withColumn(col1, idExtract(col("genres")))
    movies_df = movies_df.withColumn(col2, nameExtract(col("genres")))
    movies_df = movies_df.withColumn(col3, nameExtract(col("production_companies")))
    movies_df = movies_df.withColumn(col4, idExtract(col("production_companies")))
    movies_df = movies_df.withColumn(col5, iso3166Extract(col("production_countries")))
    movies_df = movies_df.withColumn(col6, nameExtract(col("production_countries")))
    movies_df = movies_df.withColumn(col7, iso639Extract(col("spoken_languages")))
    movies_df = movies_df.withColumn(col8, nameExtract(col("spoken_languages")))

#splitting array to seperate columns
for i in range (5):
    cityExtract = f.udf(lambda x: x[i] if x[i] != None  else None)
    colName = 'city_{}'.format(i+1)
    movies_df = movies_df.withColumn(colName, cityExtract(col("cities")))

movies_df = movies_df.drop('tagline', 'revenue', 'overview', 'genres', 'production_companies',
                           'production_countries', 'spoken_languages', 'cities')

#combining the seperate columns into good array
movies_df = movies_df.withColumn("citiesFinal", f.array('city_1','city_2','city_3','city_4','city_5'))

movies_df = movies_df.withColumn("genresFinal", f.array('genre_1_name','genre_2_name','genre_3_name','genre_4_name',
                                                        'genre_5_name','genre_6_name','genre_7_name','genre_8_name',
                                                        'genre_9_name','genre_10_name','genre_11_name','genre_12_name',
                                                        'genre_13_name','genre_14_name','genre_15_name','genre_16_name',
                                                        'genre_17_name','genre_18_name','genre_19_name','genre_20_name',
                                                        'genre_21_name','genre_22_name','genre_23_name','genre_24_name',
                                                        'genre_25_name','genre_26_name'))

movies_df = movies_df.withColumn("productionCountries", f.array('production_country_1_name','production_country_2_name',
                                                        'production_country_3_name','production_country_4_name',
                                                        'production_country_5_name','production_country_6_name',
                                                        'production_country_7_name','production_country_8_name',
                                                        'production_country_9_name','production_country_10_name',
                                                        'production_country_11_name','production_country_12_name',
                                                        'production_country_13_name','production_country_14_name',
                                                        'production_country_15_name','production_country_16_name',
                                                        'production_country_17_name','production_country_18_name',
                                                        'production_country_19_name','production_country_20_name',
                                                        'production_country_21_name','production_country_22_name',
                                                        'production_country_23_name','production_country_24_name',
                                                        'production_country_25_name','production_country_26_name'))


movies_df = movies_df.withColumn("productionCompanies", f.array('production_company_1_name','production_company_2_name',
                                                        'production_company_3_name','production_company_4_name',
                                                        'production_company_5_name','production_company_6_name',
                                                        'production_company_7_name','production_company_8_name',
                                                        'production_company_9_name','production_company_10_name',
                                                        'production_company_11_name','production_company_12_name',
                                                        'production_company_13_name','production_company_14_name',
                                                        'production_company_15_name','production_company_16_name',
                                                        'production_company_17_name','production_company_18_name',
                                                        'production_company_19_name','production_company_20_name',
                                                        'production_company_21_name','production_company_22_name',
                                                        'production_company_23_name','production_company_24_name',
                                                        'production_company_25_name','production_company_26_name'))

movies_df = movies_df.withColumn("SpokenFinal", f.array('language_1_name','language_2_name','language_3_name',
                                                        'language_4_name','language_5_name','language_6_name',
                                                        'language_7_name','language_8_name','language_9_name',
                                                        'language_10_name','language_11_name','language_12_name',
                                                        'language_13_name','language_14_name','language_15_name',
                                                        'language_16_name','language_17_name','language_18_name',
                                                        'language_19_name','language_20_name','language_21_name',
                                                        'language_22_name','language_23_name','language_24_name',
                                                        'language_25_name','language_26_name'))

#drops
for i in range(28):
    col1 = 'genre_{}_id'.format(i + 1)
    col2 = 'genre_{}_name'.format(i + 1)
    col3 = 'production_company_{}_name'.format(i + 1)
    col4 = 'production_company_{}_id'.format(i + 1)
    col5 = 'production_country_{}_iso'.format(i + 1)
    col6 = 'production_country_{}_name'.format(i + 1)
    col7 = 'language_{}_iso'.format(i + 1)
    col8 = 'language_{}_name'.format(i + 1)
    movies_df = movies_df.drop(col1, col2, col3, col4, col5, col6, col7, col8)

movies_df = movies_df.drop('city_1','city_2','city_3','city_4','city_5')

"""
yearExtractQueries = f.udf(lambda x: None if x == None else (datetime.datetime.strptime(x, "%Y").year if ('-' in x)
                                                         else datetime.datetime.strptime(x, "%Y").year))
"""

#movies_df.printSchema()
#movies_df.show(5)

#----------------------------------------------------------------------------
credits_df =  spark.read.format("csv")\
  .option("delimiter", "\t")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load("credits.csv")

prog = re.compile('\\[(.*?)\\]')
second_match = f.udf(lambda x: prog.findall(x)[1])
id_extract = f.udf(lambda x: x.split(",")[-1])

credits_df = credits_df\
  .withColumn("id", id_extract("cast,crew,id"))\
  .withColumn("cast", f.regexp_extract(f.col("cast,crew,id"), '\\[(.*?)\\]', 0))\
  .withColumn("crew", f.concat(f.lit("["),second_match("cast,crew,id"), f.lit("]")))\
  .select("cast", "crew", "id")

#credits_df.printSchema()
#credits_df.show()

#------------------------------------------------------------