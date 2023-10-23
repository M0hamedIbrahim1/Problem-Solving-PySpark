# link   : https://platform.stratascratch.com/coding/9992-find-artists-that-have-been-on-spotify-the-most-number-of-times?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Start writing code
trans1 = spotify_worldwide_daily_song_ranking
trans1 = trans1.groupby('artist').agg(F.count('*').alias('n_occurences')).orderBy(F.desc('n_occurences'))
# To validate your solution, convert your final pySpark df to a pandas df
trans1.toPandas()
