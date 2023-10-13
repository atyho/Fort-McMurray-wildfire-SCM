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
df_crc = sqlContext.read.parquet("/appdata/TU/TU_Official/CRCS_LDU/crcs_ldu_201[1-9]*_parq") \
  .select("tu_consumer_id", "Run_Date", "fsa", "Encrypted_LDU", \
          "go30", "go90", col("go91").alias("deceased"), "as115", "cvsc100") \
  .withColumn("Run_Date", to_date(concat(col("Run_Date").substr(1,4),lit("-"),col("Run_Date").substr(5,2),lit("-01")))) \
  .replace(0, None, subset="as115").replace(-8, None, subset="cvsc100").replace(0, None, subset="cvsc100")

df_crc.createOrReplaceTempView("df_crc")

# Load data for PCCF
df_pccf = spark.read.csv("pccf_can_nov2019.csv", header=True) \
  .withColumn("pstlcode", regexp_replace(col("pstlcode"),"\s+","")) \
  .withColumn("fsa", col("pstlcode").substr(1,3)) \
  .withColumn("LDU", col("pstlcode").substr(4,6))

df_pccf.createOrReplaceTempView("df_pccf")

# Load data for Fort McMurray postal codes with severe damange
df_FMD = spark.read.csv("FM_community.csv", header=True) \
  .withColumn("pstlcode", regexp_replace(col("postal_code"),"\s+",""))

df_FMD.createOrReplaceTempView("df_FMD")

##################################################
# Create age groups and refine consumer province #
##################################################

df_crc = spark.sql("SELECT *, \
                     CASE WHEN go30>0 THEN IF(go30>120, go30/12, go30) ELSE NULL END AS age, \
                     CASE WHEN fsa LIKE 'T%' THEN 'AB' \
                       WHEN fsa LIKE 'K%' OR fsa LIKE 'L%' OR fsa LIKE 'M%' OR fsa LIKE 'N%' OR fsa LIKE 'P%' THEN 'ON' \
                       WHEN fsa LIKE 'G%' OR fsa LIKE 'J%' OR fsa LIKE 'H%' THEN 'QC' \
                       WHEN fsa LIKE 'B%' THEN 'NS' \
                       WHEN fsa LIKE 'E%' THEN 'NB' \
                       WHEN fsa LIKE 'R%' THEN 'MB' \
                       WHEN fsa LIKE 'V%' THEN 'BC' \
                       WHEN fsa LIKE 'C%' THEN 'PE' \
                       WHEN fsa LIKE 'S%' THEN 'SK' \
                       WHEN fsa LIKE 'A%' THEN 'NL' \
                       WHEN fsa LIKE 'X%' THEN 'NT' \
                       WHEN fsa LIKE 'Y%' THEN 'YT' \
                       ELSE CASE WHEN go90 RLIKE '(ON|QC|NS|NB|MB|BC|PE|SK|AB|NL|NT|YT|NU)' THEN go90 ELSE NULL END \
                       END AS prov \
                   FROM df_crc ").drop("go30","go90")

df_crc.createOrReplaceTempView("df_crc")

###########################################
# Create credit tiers using credit scores #
###########################################

df_crc = spark.sql("SELECT df_crc.*, \
                     CASE \
                       WHEN as115>0 AND as115<600 THEN 'subprime' \
                       WHEN as115>=600 AND as115<700 THEN 'near prime' \
                       WHEN as115>=700 AND as115<780 THEN 'prime' \
                       WHEN as115>=780 AND as115<830 THEN 'prime plus' \
                       WHEN as115>=830 THEN 'super prime' \
                       ELSE 'NA' END AS as115_tier, \
                     CASE \
                       WHEN cvsc100>=300 and cvsc100<640 THEN 'subprime' \
                       WHEN cvsc100>=640 AND cvsc100<720 THEN 'near prime' \
                       WHEN cvsc100>=720 AND cvsc100<760 THEN 'prime' \
                       WHEN cvsc100>=760 AND cvsc100<800 THEN 'prime plus' \
                       WHEN cvsc100>=800 THEN 'super prime' \
                       ELSE 'NA' END AS cvsc100_tier \
                   FROM df_crc")

df_crc.createOrReplaceTempView("df_crc")

#####################
# Consumer Sampling #
#####################

# Create a list of consumers who has ever lived in Alberta/Saskatchewan 
# This is a coarse way to trim down the size of the data set, will further refine the target sample in later processes 
df_sample_id = df_crc.where("prov RLIKE '(AB|SK)'").select("tu_consumer_id").distinct()
df_sample_id.createOrReplaceTempView("df_sample_id")

# Only keeping the selected consumer records, also drop records for deceased consumers
df_crc = spark.sql("SELECT df_crc.* \
                   FROM df_crc \
                   INNER JOIN df_sample_id \
                     ON df_crc.tu_consumer_id = df_sample_id.tu_consumer_id \
                   WHERE deceased LIKE 'N' ").drop("deceased")

df_crc.createOrReplaceTempView("df_crc")

#############################################################################################
# Identify treated individuals to prevent changes in their postal codes DURING the disaster #
#############################################################################################

# Identify a list of individuals who lived in Fort McMurray when the wildfire began
df_pre_trt = spark.sql("SELECT tu_consumer_id, fsa, Encrypted_LDU FROM df_crc \
                       WHERE Run_Date = DATE('2016-05-01') AND fsa RLIKE '^(T9H|T9J|T9K)$' ")

df_pre_trt.createOrReplaceTempView("df_pre_trt")

# Note that those treated individuals may not live in Fort McMurray before or after the 2016 wildfire
# Also adjust their residential postal codes which might have changed during the evacuation period (minor effect)
df_crc = spark.sql("SELECT df_crc.*, \
                     CASE WHEN df_pre_trt.tu_consumer_id IS NOT NULL THEN 1 ELSE 0 END AS treated_ind, \
                     CASE WHEN df_pre_trt.tu_consumer_id IS NOT NULL AND df_crc.fsa NOT RLIKE '^(T9H|T9J|T9K)$' \
                         AND (df_crc.Run_Date BETWEEN DATE('2016-05-01') AND DATE('2016-11-01')) \
                       THEN df_pre_trt.fsa ELSE df_crc.fsa END AS fsa_adj, \
                     CASE WHEN df_pre_trt.tu_consumer_id IS NOT NULL AND df_crc.fsa NOT RLIKE '^(T9H|T9J|T9K)$' \
                       AND (df_crc.Run_Date BETWEEN DATE('2016-05-01') AND DATE('2016-11-01')) \
                       THEN df_pre_trt.Encrypted_LDU ELSE df_crc.Encrypted_LDU END AS Encrypted_LDU_adj \
                   FROM df_crc \
                   LEFT JOIN df_pre_trt \
                     ON df_crc.tu_consumer_id = df_pre_trt.tu_consumer_id ") \
         .withColumnRenamed("fsa","fsa_reported").withColumnRenamed("Encrypted_LDU","Encrypted_LDU_reported") \
         .withColumnRenamed("fsa_adj","fsa").withColumnRenamed("Encrypted_LDU_adj","Encrypted_LDU")

df_crc.createOrReplaceTempView("df_crc")

#############################
# Fort McMurray information #
#############################

# Find the minimal distance from Fort McMurray (distance in km) for each postal code in Alberta/Saskatchewan
df_dist = spark.sql("SELECT df_focal.fsa, df_focal.LDU, \
                      MIN( 111.045 * DEGREES(ACOS( LEAST(1.0, \
                           COS(RADIANS(df_focal.latitude))*COS(RADIANS(df_pair.latitude))*COS(RADIANS(df_focal.longitude - df_pair.longitude)) \
                           + SIN(RADIANS(df_focal.latitude))*SIN(RADIANS(df_pair.latitude))) )) ) AS distance_min \
                    FROM df_pccf AS df_focal \
                    CROSS JOIN (SELECT * FROM df_pccf WHERE fsa RLIKE '^(T9H|T9J|T9K)$') AS df_pair \
                    WHERE df_focal.province RLIKE '(Alberta|Saskatchewan)' \
                    GROUP BY df_focal.fsa, df_focal.LDU ")

df_dist.createOrReplaceTempView("df_dist")

df_pccf = spark.sql("SELECT df_pccf.*, df_dist.distance_min \
                    FROM df_pccf \
                    LEFT JOIN df_dist \
                      ON df_pccf.fsa = df_dist.fsa \
                      AND df_pccf.LDU = df_dist.LDU ")

df_pccf.createOrReplaceTempView("df_pccf")

# Flag Fort McMurray area with severe damange
df_pccf = spark.sql("SELECT df_pccf.*, \
                      CASE WHEN (df_pccf.fsa = df_FMD.fsa AND df_pccf.LDU = df_FMD.LDU) THEN 1 ELSE 0 END AS FM_damage \
                    FROM df_pccf \
                    LEFT JOIN df_FMD \
                      ON df_pccf.fsa = df_FMD.fsa \
                      AND df_pccf.LDU = df_FMD.LDU ")

df_pccf.createOrReplaceTempView("df_pccf")

# Load mapping key
df_mapping = spark.read.csv("dtalink.csv", header=True).where("N_repeat < 2")
df_mapping.createOrReplaceTempView("df_mapping")

# Merge with postal code mapping
df_crc = spark.sql("SELECT df_crc.*, df_mapping.LDU AS LDU_key \
                   FROM df_crc \
                   LEFT JOIN df_mapping \
                     ON df_crc.Encrypted_LDU = df_mapping.Encrypted_LDU ")

df_crc.createOrReplaceTempView("df_crc")

# Join consumer credit records with PCCF; only keep valid postal codes
df_crc = spark.sql("SELECT df_crc.*, df_pccf.FM_damage, df_pccf.distance_min \
                   FROM df_crc \
                   INNER JOIN df_pccf \
                     ON df_crc.fsa = df_pccf.fsa \
                     AND df_crc.LDU_key = df_pccf.LDU ").drop("LDU_key")

df_crc.createOrReplaceTempView("df_crc")

#########################
# Save working data set #
#########################

print('Saving data to files...')
df_crc.printSchema()
df_crc.write.parquet(path="df_crc.parquet", mode="overwrite")

# Stop the sparkContext and cluster after processing
sc.stop()
