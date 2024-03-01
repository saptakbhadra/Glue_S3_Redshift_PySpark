import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame
import logging

# logger
logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)

# Create a handler for CloudWatch
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info('============LOG MESSAGES============')

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1701277192657 = glueContext.create_dynamic_frame.from_catalog(
    database="mydatabase",
    table_name="product",
    transformation_ctx="AmazonS3_node1701277192657",
)

#Printing Schema of dynamic frame
logger.info('Printing schema of AmazonS3_node1701277192657')
AmazonS3_node1701277192657.printSchema()

#Counting no of rows in the frame
count=AmazonS3_node1701277192657.count()
print("Number of rows in AmazonS3_node1701277192657 dynamic frame: ", count)
logger.info('Count for the frame is {}'.format(count))

# Script generated for node Change Schema
ChangeSchema_node1701277256662 = ApplyMapping.apply(
    frame=AmazonS3_node1701277192657,
    mappings=[
        ("marketplace", "string", "new_marketplace", "string"),
        ("customer_id", "long", "new_customer_id", "long"),
        ("product_id", "string", "new_product_id", "string"),
        ("seller_id", "string", "new_seller_id", "string"),
        ("sell_date", "string", "new_sell_date", "string"),
        ("quantity", "long", "new_quantity", "long"),
        ("year", "string", "new_year", "string"),
    ],
    transformation_ctx="ChangeSchema_node1701277256662",
)

#Converting String values for new_seller_id  to long values using resolveChoice
ResolveChoice_node= ChangeSchema_node1701277256662.resolveChoice(specs = [('new_seller_id','cast:long')], transformation_ctx="ResolveChoice_node")

#Logging Schema of ResolveChoice_node
logger.info('Schema of ResolveChoice_node')
ResolveChoice_node.printSchema()

#Converting dynamic dataframe ResolveChoice_node into spark dataframe
logger.info('Converting dynamic dataframe ResolveChoice_node into Spark Dataframe')
spark_data_frame=ResolveChoice_node.toDF()

#Applying Spark SQL to filter
logger.info('Filtering rows where new_seller_id is not null')
spark_data_frame= spark_data_frame.where("new_seller_id is NOT NULL")


#Adding new column to the Dataframe
logger.info('create new column Status with Active values')
spark_data_frame_filter=spark_data_frame.withColumn("new_status", lit("Active"))

spark_data_frame_filter.show()

logger.info('Converting Spark Dataframe into table view_product')
spark_data_frame_filter.createOrReplaceTempView("product_view")

logger.info('Creating Dataframe by spark sql')

#Aggregation based on year
product_sql_df= spark.sql("SELECT new_year, count(new_customer_id) as cnt,sum(new_quantity) as qty FROM product_view group by new_year ")

logger.info('display records after aggregate result')
product_sql_df.show()

#Convert the data frame back to a dynamic frame
logger.info('Converting Spark Dataframe to Dynamic frame')
dynamic_frame= DynamicFrame.fromDF(product_sql_df, glueContext, "dynamic_frame")

logger.info('dynamic frame uploaded in bucket s3://myglue-etl-project-1/output/')

# Script generated for node Amazon S3
AmazonS3_node1701277282846 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://myglue-etl-project-1/output/",
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="AmazonS3_node1701277282846",
)

job.commit()
