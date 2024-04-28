import pyspark.sql.functions as F

def prepare_data(spark, ratings_data_path):
  """
  Loads and preprocesses movie ratings data.

  Args:
      spark (SparkSession): SparkSession object.
      ratings_data_path (str): Path to the ratings dataset.

  Returns:
      pyspark.sql.DataFrame: Preprocessed DataFrame for movie recommendations.
  """

  ratings_df = spark.read.csv(ratings_data_path, header=True)

  # Handle potential null values or data quality issues (adapt based on your data)
  clean_ratings_df = ratings_df.dropna(subset=["userId", "movieId", "rating"])

  # Pivot the DataFrame to create a user-movie rating matrix
  rating_matrix = clean_ratings_df.groupBy("userId").pivot("movieId").agg(F.avg("rating").alias("rating"))

  return rating_matrix
