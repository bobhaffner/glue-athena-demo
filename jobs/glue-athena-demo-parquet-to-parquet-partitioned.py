import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "glue_athena_demo", table_name = "nyc_taxi_parquet", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "glue_athena_demo", table_name = "nyc_taxi_parquet", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("id", "string", "id", "string"), ("vendor_id", "int", "vendor_id", "int"), ("pickup_datetime", "timestamp", "pickup_datetime", "timestamp"), ("dropoff_datetime", "timestamp", "dropoff_datetime", "timestamp"), ("passenger_count", "int", "passenger_count", "int"), ("pickup_longitude", "double", "pickup_longitude", "double"), ("pickup_latitude", "double", "pickup_latitude", "double"), ("dropoff_longitude", "double", "dropoff_longitude", "double"), ("dropoff_latitude", "double", "dropoff_latitude", "double"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("trip_duration", "int", "trip_duration", "int")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("id", "string", "id", "string"), ("vendor_id", "int", "vendor_id", "int"), ("pickup_datetime", "timestamp", "pickup_datetime", "timestamp"), ("dropoff_datetime", "timestamp", "dropoff_datetime", "timestamp"), ("passenger_count", "int", "passenger_count", "int"), ("pickup_longitude", "double", "pickup_longitude", "double"), ("pickup_latitude", "double", "pickup_latitude", "double"), ("dropoff_longitude", "double", "dropoff_longitude", "double"), ("dropoff_latitude", "double", "dropoff_latitude", "double"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("trip_duration", "int", "trip_duration", "int")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://ids-glue-athena-demo/parquet_partitioned_test"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]

def get_month(df):
    df["month"] = str(df["pickup_datetime"])[5:7]
    return df

applymapping2 = Map.apply(frame = dropnullfields3, f = get_month) 

datasink4 = glueContext.write_dynamic_frame.from_options(frame = applymapping2, connection_type = "s3", connection_options = {"path": "s3://ids-glue-athena-demo/nyc_taxi_parquet_partitioned", "partitionKeys":["month"]}, format = "parquet", transformation_ctx = "datasink4")
job.commit()