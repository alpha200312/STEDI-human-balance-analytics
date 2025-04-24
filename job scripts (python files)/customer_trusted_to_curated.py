import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1745297993805 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1745297993805")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1745297995194 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1745297995194")

# Script generated for node Join
Join_node1745298000293 = Join.apply(frame1=AWSGlueDataCatalog_node1745297995194, frame2=AWSGlueDataCatalog_node1745297993805, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1745298000293")

# Script generated for node Drop Duplicates
DropDuplicates_node1745298656341 =  DynamicFrame.fromDF(Join_node1745298000293.toDF().dropDuplicates(["email"]), glueContext, "DropDuplicates_node1745298656341")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1745298656341, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745297924264", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745298016036 = glueContext.getSink(path="s3://udacity-lake1/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745298016036")
AmazonS3_node1745298016036.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1745298016036.setFormat("json")
AmazonS3_node1745298016036.writeFrame(DropDuplicates_node1745298656341)
job.commit()