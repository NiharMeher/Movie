import logging

def setup_logger(name, log_level=logging.INFO):
  """Sets up a logger with a specific name and log level."""
  logger = logging.getLogger(name)
  logger.setLevel(log_level)
  handler = logging.StreamHandler()
  formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
  handler.setFormatter(formatter)
  logger.addHandler(handler)
  return logger

def load_data(spark, data_path, file_format="csv", header=True):
  """Loads data from a file or directory using PySpark."""
  # Add flexibility to handle different data formats and options
  return spark.read.format(file_format).option("header", header).load(data_path)
