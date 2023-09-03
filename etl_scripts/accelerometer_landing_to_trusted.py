import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1693458859728 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1693458859728",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ud-aws-lakehouse/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Select Fields
SelectFields_node1693458889794 = SelectFields.apply(
    frame=AWSGlueDataCatalog_node1693458859728,
    paths=["email"],
    transformation_ctx="SelectFields_node1693458889794",
)

# Script generated for node Join
Join_node1693458927228 = Join.apply(
    frame1=SelectFields_node1693458889794,
    frame2=S3bucket_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1693458927228",
)

# Script generated for node Drop Fields
DropFields_node1693459000094 = DropFields.apply(
    frame=Join_node1693458927228,
    paths=["email"],
    transformation_ctx="DropFields_node1693459000094",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://ud-aws-lakehouse/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="accelerometer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1693459000094)
job.commit()
