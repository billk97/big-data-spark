from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql.types import *

spark = SparkSession.builder.appName("import").config("spark.some.config.option", "some-value").getOrCreate()
spark.sparkContext.setLogLevel("")
sc = spark.sparkContext 

borrowers_txt = sc.textFile("BORROWERS.TXT")
loans_txt = sc.textFile("LOANS.TXT")

borrowers = borrowers_txt.coalesce(1).map(lambda line: line.split("|"))
headers = borrowers.first()
borrowers = borrowers.filter(lambda line: line != headers)
borrowers = borrowers.map(lambda line: (int(line[0]), line[1], line[2]))

borrowers_schema = StructType([
    StructField(headers[0], IntegerType(), True),
    StructField(headers[1], StringType(), True),
    StructField(headers[2], StringType(), True)
])

loans = loans_txt.coalesce(1).map(lambda line: line.split("|"))
headers = loans.first()
loans = loans.filter(lambda line: line != headers)
loans = loans.map(lambda line: (int(line[0]), line[1], int(line[2]), line[3]))

# last value is true means it's allowed to be null
loans_schema = StructType([
    StructField(headers[0], IntegerType(), True),
    StructField(headers[1], StringType(), True),
    StructField(headers[2], IntegerType(), True),
    StructField(headers[3], TimestampType(), True)
])

borrowers_DF = spark.createDataFrame(borrowers, borrowers_schema)
borrowers_DF.createOrReplaceTempView("borrowersTable")
query = spark.sql("""SELECT * FROM borrowersTable """)

query.write.format("csv").save("borrowersTable")

# borrower_df = borrowers.toDF()
# borrower_df.write.csv("borrowerCSV")

