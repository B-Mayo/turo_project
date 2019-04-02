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
## @args: [database = "*******", table_name = "*******", transformation_ctx = "*******"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "*******", table_name = "*******", transformation_ctx = "*******")
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "*******", table_name = "*******", transformation_ctx = "*******")
datasource2 = glueContext.create_dynamic_frame.from_catalog(database = "*******", table_name = "*******", transformation_ctx = "*******")


partitioned_df = datasource0.toDF().repartition(1)
partitioned_df = partitioned_df.filter(partitioned_df.car_page_url !="car_page_url").repartition(1)
partitioned_df = partitioned_df.filter(partitioned_df.car_page_url !="").repartition(1)
partitioned_df = partitioned_df.filter(partitioned_df.car_page_url !=" ").repartition(1)
partitioned_df = partitioned_df.orderBy(["todays_date"], ascending=False).dropDuplicates(subset=['car_page_url']).repartition(1)

partitioned1_df = datasource1.toDF().repartition(1)

ta = partitioned_df.alias('ta')
tb = partitioned1_df.alias('tb')
tc = ta.join(tb, [ta.car_page_url==tb.car_url], "inner")

tc = tc.dropDuplicates(['car_page_url']).repartition(1)
tc = tc.filter(partitioned_df.car_page_url !="car_page_url").repartition(1)
tc = tc.filter(partitioned_df.car_page_url !="").repartition(1)
tc = tc.filter(partitioned_df.car_page_url !=" ").repartition(1)
tc = tc.orderBy(["todays_date"], ascending=False).dropDuplicates(subset=['car_page_url']).repartition(1)

partitioned2_df = datasource2.toDF().repartition(1)

df1 = partitioned2_df.select('page_url','host_id')

ta = tc.alias('ta')
tb = df1.alias('tb')
tc = ta.join(tb, [ta.host_url==tb.page_url], "inner")
tc = tc.dropDuplicates(['car_page_url']).repartition(1)


datasource0 = DynamicFrame.fromDF(tc, glueContext, "datasource0")

## @type: ApplyMapping
## @args: [mapping = [("vehicle_type", "string", "vehicle_type", "string"),("host_id", "int", "host_id", "int"),("description", "string", "description", "string"), ("delivery_airport", "string", "delivery_airport", "string"),("state", "string", "state", "string"), ("city", "string", "city", "string"), ("make", "string", "make", "string"), ("model", "string", "model", "string"),("mpg", "string", "mpg", "string"), ("n_of_doors", "string", "n_of_doors", "string"), ("n_of_seats", "string", "n_of_seats", "string"), ("gas", "string", "gas", "string"), ("distance_included", "string", "distance_included", "string"), ("insurance", "string", "insurance", "string"), ("car_year", "string", "car_year", "string"), ("zip_code", "string", "zip_code", "string"), ("delivery_cost_airport", "string", "delivery_cost_airport", "string"), ("add_delivery_info", "string", "add_delivery_info", "string"), ("add_delivery_cost", "string", "add_delivery_cost", "string"), ("car_page_url", "string", "car_page_url", "string"), ("star_rating", "string", "star_rating", "string"), ("trim", "string", "trim", "string"), ("guidelines", "string", "guidelines", "string"), ("business_class", "string", "business_class", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("vehicle_type", "string", "vehicle_type", "string"),("host_id", "int", "host_id", "int"),("description", "string", "description", "string"), ("delivery_airport", "string", "delivery_airport", "string"), ("state", "string", "state", "string"), ("city", "string", "city", "string"), ("make", "string", "make", "string"), ("model", "string", "model", "string"),("mpg", "string", "mpg", "string"), ("n_of_doors", "string", "n_of_doors", "string"), ("n_of_seats", "string", "n_of_seats", "string"), ("gas", "string", "gas", "string"), ("distance_included", "string", "distance_included", "string"), ("insurance", "string", "insurance", "string"), ("car_year", "string", "car_year", "string"), ("zip_code", "string", "zip_code", "string"), ("delivery_cost_airport", "string", "delivery_cost_airport", "string"), ("add_delivery_info", "string", "add_delivery_info", "string"), ("add_delivery_cost", "string", "add_delivery_cost", "string"), ("car_page_url", "string", "car_page_url", "string"), ("star_rating", "string", "star_rating", "string"), ("trim", "string", "trim", "string"), ("guidelines", "string", "guidelines", "string"), ("business_class", "string", "business_class", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["insurance","host_id", "car_year", "city", "extras", "zip_code", "features", "trim", "star_rating", "gas", "guidelines", "model", "state", "description","add_delivery_cost", "car_id", "make", "mpg", "n_of_seats", "delivery_cost_airport", "vehicle_type", "distance_included", "n_of_doors", "add_delivery_info", "car_page_url", "business_class", "delivery_airport"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["insurance", "host_id","car_year", "city", "extras", "zip_code","description", "features", "trim", "star_rating", "gas", "guidelines", "model", "state", "add_delivery_cost", "car_id", "make", "mpg", "n_of_seats", "delivery_cost_airport", "vehicle_type", "distance_included", "n_of_doors", "add_delivery_info", "car_page_url", "business_class", "delivery_airport"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "*******", table_name = "*******", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "*******", table_name = "*******", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
## @type: DataSink
## @args: [database = "*******", table_name = "*******", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "*******", table_name = "*******", transformation_ctx = "datasink5")
job.commit()
