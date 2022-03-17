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
df_data = sqlContext.read.parquet("/home/mfa/hoso/TU_files/climate_chg/sample_aggregate/df_cdb.parquet")
df_data.createOrReplaceTempView("df_data")

#########################################################
# Create a sample dataset of consumer ID using CRC data #
#########################################################

df_cid_sample = df_data.select("TU_Consumer_ID").distinct().sample(False, 0.001, 1983)
df_cid_sample.createOrReplaceTempView("df_cid_sample")

# Sampling account files
df_data = spark.sql("SELECT df_data.* \
                    FROM df_data \
                    INNER JOIN df_cid_sample \
                      ON df_data.TU_Consumer_ID = df_cid_sample.TU_Consumer_ID")

df_data.createOrReplaceTempView("df_data")

###########################
# Save dataframe to files #
###########################

print('Saving data to files...')
df_data.printSchema()
df_data.write.csv(path="tu_acct_sample_folder", mode="overwrite", sep=",", header="true")

# Stop the sparkContext and cluster after processing
sc.stop()
