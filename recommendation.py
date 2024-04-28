from pyspark.ml.recommendation import ALS

def generate_recommendations(rating_matrix, num_recommendations, reg_param):
  """
  Trains an ALS model and generates recommendations for users.

  Args:
      rating_matrix (pyspark.sql.DataFrame): User-movie rating matrix.
      num_recommendations (int): Number of recommendations to generate per user.
      reg_param (float): Regularization parameter for ALS model.

  Returns:
      pyspark.sql.DataFrame: DataFrame containing user ID and recommended movies.
  """

  # Train an ALS model for movie recommendation
  als = ALS(rank=10, regParam=reg_param, seed=5)
  model = als.fit(rating_matrix)

  # Generate top movie recommendations for each user
  user_recs = model.recommendForAllUsers(num_recommendations)

  return user_recs
