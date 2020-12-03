from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types, HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import to_date

spark = SparkSession.builder.appName("import").config("spark.some.config.option", "some-value").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext 

borrowers_txt = sc.textFile("BORROWERS.TXT")
borrowers = borrowers_txt.coalesce(1).map(lambda line: line.split("|"))
headers = borrowers.first()
borrowers = borrowers.filter(lambda line: line != headers)
borrowers = borrowers.map(lambda line: (int(line[0]), line[1], line[2]))

borrowers_schema = StructType([
    StructField(headers[0], IntegerType(), True),
    StructField(headers[1], StringType(), True),
    StructField(headers[2], StringType(), True)
])

borrowers_DF = spark.createDataFrame(borrowers, borrowers_schema)
borrowers_DF.createOrReplaceTempView("borrowersTable")
query = spark.sql("""SELECT * FROM borrowersTable """)

query.write.format("csv").save("borrowersTable")

loans_txt = sc.textFile("LOANS.TXT")
loans = loans_txt.coalesce(1).map(lambda line: line.split("|"))
headers = loans.first()
loans = loans.filter(lambda line: line != headers)
loans = loans.map(lambda line: (int(line[0]),line[1],int(line[2]),line[3]))

# last value is true means it's allowed to be null
loans_schema = StructType([
    StructField(headers[0], IntegerType(), True),
    StructField(headers[1], StringType(), True),
    StructField(headers[2], IntegerType(), True),
    StructField(headers[3], StringType(), True)
])

loans_DF = spark.createDataFrame(loans, loans_schema)
loans_DF.select(to_date(loans_DF.date_key, "yyyy-MM-dd").alias("date_key")).collect()
loans_DF.createOrReplaceTempView("loansTable")
query = spark.sql("""SELECT * FROM loansTable""")
query.write.format("csv").save("loansTable")

#creation join table
bt = borrowers_DF.alias("bt")
lt = loans_DF.alias("lt")
# join = bt.join(lt, bt.bid == lt.bid)
join = bt.join(lt, "bid")
join.createOrReplaceTempView("join")


qube_all = spark.sql("SELECT department, gender, SUM(bid) as bid_count FROM join  GROUP BY GROUPING SETS((department, gender), (department), (gender), ())")
qube_all = qube_all.coalesce(1)
qube_all.write.format("csv").save("all_qube")

qube_both = spark.sql("SELECT department, gender, SUM(bid) FROM join GROUP BY department, gender")
qube_both = qube_both.coalesce(1)
qube_both.write.format("csv").save("department_gender")

qube_dep = spark.sql("SELECT department, SUM(bid) FROM join GROUP BY department")
qube_dep = qube_dep.coalesce(1)
qube_dep.write.format("csv").save("department")

qube_gent = spark.sql("SELECT gender, SUM(bid) FROM join GROUP BY gender")
qube_gent = qube_gent.coalesce(1)
qube_gent.write.format("csv").save("gender")


# erotima 2
t = qube_both.alias("t")
tm = t.select("department", t.gender, t.bid_count).where(t.gender == "M")
tf = t.select("department", t.gender, t.bid_count).where(t.gender == "F")
tf = tf.withColumnRename("bid_count", "bid_count_F")
tf.join(tm, "department")
joined.select(joined.department).where("bid_count_F" > "bid_counr").show()