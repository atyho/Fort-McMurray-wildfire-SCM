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
df_data = sqlContext.read.parquet("../sample_trade/ML/df_acct.parquet")
df_data.createOrReplaceTempView("df_data")

# Load files for CRC data
df_crc = sqlContext.read.parquet("../sample_crc/df_cdb.parquet")
df_crc.createOrReplaceTempView("df_crc")

# Join CRC data with individual loan data
df_data = spark.sql("SELECT df_data.*, df_crc.fsa, df_crc.FM_damage \
                    FROM df_data \
                    LEFT JOIN df_crc \
                      ON df_data.TU_Consumer_ID = df_crc.tu_consumer_id \
                      AND df_data.Run_date = df_crc.Run_Date ")

df_data.createOrReplaceTempView("df_data")

#########################################################
# Create a sample dataset of consumer ID using CRC data #
#########################################################

df_cid_sample = df_data.where("FM_damage==1").select("TU_Trade_ID").distinct().sample(False, 1.0, 1983)
df_cid_sample.createOrReplaceTempView("df_cid_sample")

# Sampling account files
df_data = spark.sql("SELECT df_data.* \
                    FROM df_data \
                    INNER JOIN df_cid_sample \
                      ON df_data.TU_Trade_ID = df_cid_sample.TU_Trade_ID ")

df_data.createOrReplaceTempView("df_data")

###########################
# Save dataframe to files #
###########################

print('Saving data to files...')
df_data.printSchema()
df_data.write.csv(path="tu_acct_sample_folder", mode="overwrite", sep=",", header="true")

# Stop the sparkContext and cluster after processing
sc.stop()
