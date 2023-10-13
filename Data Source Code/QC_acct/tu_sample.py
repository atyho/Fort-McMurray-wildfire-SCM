from pyspark.sql import SQLContext, SparkSession, Row
from pyspark import *
from pyspark.sql.functions import * # Call SparkSQL native functions such as abs, sqrt...
from pyspark.sql.types import DateType

spark=SparkSession\
.builder\
.appName("Python Spark Dataframe")\
.config(conf = SparkConf())\
.getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

########################
# Loading source files #
########################

# Load files for data
df_acct = sqlContext.read.parquet("../sample_trade/ML/df_acct.parquet")
df_acct.createOrReplaceTempView("df_acct")

# Load files for CRC data
df_crc = sqlContext.read.parquet("../sample_crc/df_crc.parquet")
df_crc.createOrReplaceTempView("df_crc")

#########################################################
# Create a sample dataset of consumer ID using CRC data #
#########################################################

# Sampling CRC files
#df_data = spark.sql("SELECT * FROM df_crc WHERE tu_consumer_id IN (358272014, 999535902) ")
#tu_consumer_id IN (358272014, 999535902, 272476822, 439290455)
#df_data.createOrReplaceTempView("df_data")

# Sampling account files
#df_data = spark.sql("SELECT * FROM df_acct WHERE TU_Consumer_ID IN (272476822, 439290455) ")
#TU_Consumer_ID IN (358272014, 999535902, 272476822, 439290455)
#df_data.createOrReplaceTempView("df_data")

df_cid_sample = df_acct.where("treated_ind = 1").select("TU_Trade_ID").distinct().sample(False, 1.0, 1983)
df_cid_sample.createOrReplaceTempView("df_cid_sample")

# Sampling account files
df_data = spark.sql("SELECT df_acct.* \
                   FROM df_acct \
                   INNER JOIN df_cid_sample \
                     ON df_acct.TU_Trade_ID = df_cid_sample.TU_Trade_ID ")

df_data.createOrReplaceTempView("df_data")

###########################
# Save dataframe to files #
###########################

print('Saving data to files...')
df_data.printSchema()
df_data.write.csv(path="tu_acct_sample_folder", mode="overwrite", sep=",", header="true")

# Stop the sparkContext and cluster after processing
sc.stop()
