import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1704683542162 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://myglue-etl-project-1/output/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1704683542162",
)

# Script generated for node Change Schema
ChangeSchema_node1704683699557 = ApplyMapping.apply(
    frame=AmazonS3_node1704683542162,
    mappings=[
        ("new_year", "string", "year", "VARCHAR"),
        ("cnt", "long", "no_of_customer", "BIGINT"),
        ("qty", "long", "quantity", "BIGINT"),
    ],
    transformation_ctx="ChangeSchema_node1704683699557",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1704683820499 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1704683699557,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-391491144836-ca-central-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.product_tab_def",
        "connectionName": "Redshift connection",
        "preactions": "CREATE TABLE IF NOT EXISTS public.product_tab_def (year VARCHAR, no_of_customer BIGINT, quantity BIGINT);",
    },
    transformation_ctx="AmazonRedshift_node1704683820499",
)

job.commit()
