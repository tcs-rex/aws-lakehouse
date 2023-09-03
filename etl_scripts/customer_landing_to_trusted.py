import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows
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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ud-aws-lakehouse/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Remove Null Rows
RemoveNullRows_node1693365984188 = S3bucket_node1.gs_null_rows()

# Script generated for node Drop Duplicates
DropDuplicates_node1693365975904 = DynamicFrame.fromDF(
    RemoveNullRows_node1693365984188.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1693365975904",
)

# Script generated for node SQL Query
SqlQuery128 = """
select * from myDataSource
where shareWithResearchAsOfDate != 0;
"""
SQLQuery_node1693458155101 = sparkSqlQuery(
    glueContext,
    query=SqlQuery128,
    mapping={"myDataSource": DropDuplicates_node1693365975904},
    transformation_ctx="SQLQuery_node1693458155101",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://ud-aws-lakehouse/customer-trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="customer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(SQLQuery_node1693458155101)
job.commit()
