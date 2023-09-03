import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node trainer_data_trusted
trainer_data_trusted_node1693753196421 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="trainer_data_trusted",
    transformation_ctx="trainer_data_trusted_node1693753196421",
)

# Script generated for node customer_curated
customer_curated_node1693619191362 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1693619191362",
)

# Script generated for node accel_trusted
accel_trusted_node1693619192060 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="accelerometer_trusted",
    transformation_ctx="accel_trusted_node1693619192060",
)

# Script generated for node SQL Query
SqlQuery44 = """
select * from myDataSource where 
sensorreadingtime IS NOT NULL AND serialnumber IS NOT NULL;
"""
SQLQuery_node1693753696522 = sparkSqlQuery(
    glueContext,
    query=SqlQuery44,
    mapping={"myDataSource": trainer_data_trusted_node1693753196421},
    transformation_ctx="SQLQuery_node1693753696522",
)

# Script generated for node Change Schema
ChangeSchema_node1693619387529 = ApplyMapping.apply(
    frame=customer_curated_node1693619191362,
    mappings=[
        ("serialnumber", "string", "customerserialnumber", "string"),
        ("email", "string", "email", "string"),
    ],
    transformation_ctx="ChangeSchema_node1693619387529",
)

# Script generated for node Join
Join_node1693619458296 = Join.apply(
    frame1=accel_trusted_node1693619192060,
    frame2=ChangeSchema_node1693619387529,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1693619458296",
)

# Script generated for node Drop Fields
DropFields_node1693620041992 = DropFields.apply(
    frame=Join_node1693619458296,
    paths=["email"],
    transformation_ctx="DropFields_node1693620041992",
)

# Script generated for node SQL Query
SqlQuery45 = """
select * from myDataSource
WHERE timestamp IS NOT NULL AND customerserialnumber IS NOT NULL;
"""
SQLQuery_node1693753993857 = sparkSqlQuery(
    glueContext,
    query=SqlQuery45,
    mapping={"myDataSource": DropFields_node1693620041992},
    transformation_ctx="SQLQuery_node1693753993857",
)

# Script generated for node Join
Join_node1693752118880 = Join.apply(
    frame1=SQLQuery_node1693753696522,
    frame2=SQLQuery_node1693753993857,
    keys1=["sensorreadingtime", "serialnumber"],
    keys2=["timestamp", "customerserialnumber"],
    transformation_ctx="Join_node1693752118880",
)

# Script generated for node Change Schema
ChangeSchema_node1693752158339 = ApplyMapping.apply(
    frame=Join_node1693752118880,
    mappings=[
        ("z", "double", "z", "double"),
        ("y", "double", "y", "double"),
        ("x", "double", "x", "double"),
        ("sensorreadingtime", "long", "sensorreadingtime", "long"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("distancefromobject", "float", "distancefromobject", "float"),
    ],
    transformation_ctx="ChangeSchema_node1693752158339",
)

# Script generated for node Amazon S3
AmazonS3_node1693752534718 = glueContext.getSink(
    path="s3://ud-aws-lakehouse/ML_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1693752534718",
)
AmazonS3_node1693752534718.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="ML_curated2"
)
AmazonS3_node1693752534718.setFormat("json")
AmazonS3_node1693752534718.writeFrame(ChangeSchema_node1693752158339)
job.commit()
