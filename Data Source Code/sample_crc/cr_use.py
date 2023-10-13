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

# Comments / Remarks

###########################
# Load files for CRC data #
###########################

# Load files for CRC data
df_crc = sqlContext.read.parquet("df_crc.parquet")
df_crc.createOrReplaceTempView("df_crc")
df_crc.printSchema()

# Load sample tenure file
df_tenure = sqlContext.read.parquet("df_tenure.parquet")
df_tenure.createOrReplaceTempView("df_tenure")
df_tenure.printSchema()

######################################
# Append CRC data with other TU data #
######################################

# Prepare data set for joining with CRC
df_tenure = df_tenure.withColumnRenamed("tu_consumer_id","tu_consumer_id_append").withColumnRenamed("Run_Date","Run_Date_append").drop("fsa","LDU")
df_tenure.createOrReplaceTempView("df_tenure")

# Join CRC data with individual loan data
df_cdb = spark.sql("SELECT df_crc.*, df_tenure.* \
                   FROM df_crc \
                   LEFT JOIN df_tenure \
                     ON df_crc.tu_consumer_id = df_tenure.tu_consumer_id_append \
                     AND df_crc.Run_Date = df_tenure.Run_Date_append ") \
         .drop("tu_consumer_id_append", "Run_Date_append")

df_cdb.createOrReplaceTempView("df_cdb")

###################################################
# Adjust tenure data by consumer-level HELOC data #
###################################################

# Load consumer-level HELOC file
df_heloc = sqlContext.read.parquet("../sample_trade/HELOC/df_ind.parquet") \
  .select("TU_Consumer_ID", "Ref_date", "heloc_bal", "heloc_lmt", "N_heloc")

df_heloc.createOrReplaceTempView("df_heloc")

# Join derived data with consumer-level HELOC data
df_cdb = spark.sql("SELECT df_cdb.*, \
                     df_heloc.heloc_bal, df_heloc.heloc_lmt, df_heloc.N_heloc \
                   FROM df_cdb \
                   LEFT JOIN df_heloc \
                     ON df_cdb.tu_consumer_id = df_heloc.TU_Consumer_ID \
                     AND YEAR(df_cdb.Run_Date) = YEAR(df_heloc.Ref_date) \
                     AND MONTH(df_cdb.Run_Date) = MONTH(df_heloc.Ref_date) ")

df_cdb.createOrReplaceTempView("df_cdb")

# Adjust homewonership indicator based on HELOC existence (currently active)
df_cdb = spark.sql("SELECT df_cdb.*, \
                     CASE WHEN IFNULL(df_cdb.N_heloc,0)>0 OR df_cdb.homeowner_mrtg=1 THEN 1 ELSE 0 END AS homeowner \
                   FROM df_cdb ")

df_cdb.createOrReplaceTempView("df_cdb")

#########################################################
# Append derived data with consumer-level mortgage data #
#########################################################

# Load consumer-level mortgage file
df_ml = sqlContext.read.parquet("../sample_trade/ML/df_ind.parquet") \
  .select("TU_Consumer_ID", "Ref_date", "ml_bal", "N_ml")

df_ml.createOrReplaceTempView("df_ml")

# Join derived data with consumer-level mortgage data
df_cdb = spark.sql("SELECT df_cdb.*, \
                     df_ml.ml_bal, df_ml.N_ml \
                   FROM df_cdb \
                   LEFT JOIN df_ml \
                     ON df_cdb.tu_consumer_id = df_ml.tu_consumer_id \
                     AND YEAR(df_cdb.Run_Date) = YEAR(df_ml.Ref_date) \
                     AND MONTH(df_cdb.Run_Date) = MONTH(df_ml.Ref_date) ")

df_cdb.createOrReplaceTempView("df_cdb")

#########################
# Save working data set #
#########################

print('Saving data to files...')
df_cdb.printSchema()
df_cdb.write.parquet(path="df_cdb.parquet", mode="overwrite")

# Stop the sparkContext and cluster after processing
sc.stop()
