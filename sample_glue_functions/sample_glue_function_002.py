import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "*********", table_name = "***********", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "***********", table_name = "***********", transformation_ctx = "datasource0")
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "***********", table_name = "db_***********_***********", transformation_ctx = "datasource1")

partitioned_df = datasource0.toDF().repartition(1)
partitioned_df = partitioned_df.filter(partitioned_df.car_page_url !="car_page_url").repartition(1)
partitioned_df = partitioned_df.filter(partitioned_df.car_page_url !="").repartition(1)
partitioned_df = partitioned_df.filter(partitioned_df.car_page_url !=" ").repartition(1)
partitioned1_df = datasource1.toDF().repartition(1)

df = partitioned_df
df1 = partitioned1_df.select(col('car_page_url').alias('car_url'),'car_id')

df = df.join(df1, [df.car_page_url==df1.car_url],"inner")

datasource0 = DynamicFrame.fromDF(df, glueContext, "datasource0")

## @type: ApplyMapping
## @args: [mapping = [("car_id", "int", "car_id", "int"), ("features", "string", "features", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("car_id", "int", "car_id", "int"), ("features", "string", "features", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["features", "car_id"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["features", "car_id"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "***********", table_name = "db_***********_***********", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "***********", table_name = "db_***********_***********", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
## @type: DataSink
## @args: [database = "***********", table_name = "db_***********_***********", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "***********", table_name = "db_***********_***********", transformation_ctx = "datasink5")
job.commit()
