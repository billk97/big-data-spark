from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("import").config("spark.some.config.option", "some-value").getOrCreate()
spark.sparkContext.setLogLevel("")
sc = spark.sparkContext 

borrowers_txt = sc.textFile("BORROWERS.TXT")
seperated_borrowers = borrowers_txt.coalesce(1).map(lambda line: line.split("|"))
borrower_df = seperated_borrowers.toDF()
borrower_df.write.csv("borrowerCSV")

