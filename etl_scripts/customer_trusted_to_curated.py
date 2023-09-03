import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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

# Script generated for node customer_trusted
customer_trusted_node1693603385673 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1693603385673",
)

# Script generated for node accel_trusted
accel_trusted_node1693603311555 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="accelerometer_trusted",
    transformation_ctx="accel_trusted_node1693603311555",
)

# Script generated for node SQL Query
SqlQuery18 = """
select * from myDataSource where timestamp > 0;

"""
SQLQuery_node1693603570234 = sparkSqlQuery(
    glueContext,
    query=SqlQuery18,
    mapping={"myDataSource": accel_trusted_node1693603311555},
    transformation_ctx="SQLQuery_node1693603570234",
)

# Script generated for node Select Fields
SelectFields_node1693604361665 = SelectFields.apply(
    frame=SQLQuery_node1693603570234,
    paths=["user"],
    transformation_ctx="SelectFields_node1693604361665",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1693604434665 = DynamicFrame.fromDF(
    SelectFields_node1693604361665.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1693604434665",
)

# Script generated for node Join
Join_node1693603510866 = Join.apply(
    frame1=customer_trusted_node1693603385673,
    frame2=DropDuplicates_node1693604434665,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1693603510866",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://ud-aws-lakehouse/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="customer_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(Join_node1693603510866)
job.commit()
