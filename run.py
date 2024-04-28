from pyspark.sql import SparkSession
from recommendation import generate_recommendations
from data_prep import prepare_data
import config  # Import configuration

def main():
  """Sets up Spark, loads data, trains the model, and generates recommendations."""

  # Use configuration from config.py
  spark = SparkSession.builder.appName("MovieRecommendation").master(config.SPARK_MASTER).getOrCreate()

  # Set data paths based on configuration
  ratings_data_path = config.DATA_PATH + "/ratings.csv"  # Modify as needed

  # Load and preprocess data
  rating_matrix = prepare_data(spark, ratings_data_path)

  # Generate recommendations
  num_recommendations = 10
  reg_param = 0.1  # Adjust regularization parameter as needed
  recommendations_df = generate_recommendations(rating_matrix, num_recommendations, reg_param)

  # ... rest of the code (display recommendations or save to file)

if __name__ == "__main__":
  main()
