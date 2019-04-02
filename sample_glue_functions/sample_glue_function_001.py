import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session


job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "********", table_name = "db_********_********", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "********", table_name = "db_********_********", transformation_ctx = "datasource0")
partitioned_df = datasource0.toDF().repartition(1)
datasource0 = DynamicFrame.fromDF(partitioned_df, glueContext, "datasource0")


## @type: ApplyMapping
## @args: [mapping = [("page_url", "string", "page_url", "string"), ("host_id", "int", "host_id", "int")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("page_url", "string", "page_url", "string"), ("host_id", "int", "host_id", "int")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "********************"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "*****************************"}, format = "csv", transformation_ctx = "datasink2")
job.commit()
