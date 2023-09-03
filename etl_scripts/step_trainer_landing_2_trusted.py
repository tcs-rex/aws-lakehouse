import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node trainer_landing_bucket
trainer_landing_bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ud-aws-lakehouse/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="trainer_landing_bucket_node1",
)

# Script generated for node customer_curated_table
customer_curated_table_node1693608471747 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi-db",
        table_name="customer_curated",
        transformation_ctx="customer_curated_table_node1693608471747",
    )
)

# Script generated for node xfrm data
xfrmdata_node1693609303523 = ApplyMapping.apply(
    frame=trainer_landing_bucket_node1,
    mappings=[
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "float"),
    ],
    transformation_ctx="xfrmdata_node1693609303523",
)

# Script generated for node xfrm customers
xfrmcustomers_node1693609555147 = ApplyMapping.apply(
    frame=customer_curated_table_node1693608471747,
    mappings=[("serialnumber", "string", "customer_serialnumber", "string")],
    transformation_ctx="xfrmcustomers_node1693609555147",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1693610279522 = DynamicFrame.fromDF(
    xfrmcustomers_node1693609555147.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1693610279522",
)

# Script generated for node Join
Join_node1693608504414 = Join.apply(
    frame1=DropDuplicates_node1693610279522,
    frame2=xfrmdata_node1693609303523,
    keys1=["customer_serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1693608504414",
)

# Script generated for node Change Schema
ChangeSchema_node1693609625259 = ApplyMapping.apply(
    frame=Join_node1693608504414,
    mappings=[
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("distanceFromObject", "float", "distanceFromObject", "float"),
    ],
    transformation_ctx="ChangeSchema_node1693609625259",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://ud-aws-lakehouse/steptrainer--trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="trainer_data_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(ChangeSchema_node1693609625259)
job.commit()
