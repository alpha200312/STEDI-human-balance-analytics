import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import re

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

# Script generated for node Amazon S3
AmazonS3_node1745260010351 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-lake1/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1745260010351")

# Script generated for node PrivacyFilter
PrivacyFilter_node1745260782345 = Filter.apply(frame=AmazonS3_node1745260010351, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="PrivacyFilter_node1745260782345")

# Script generated for node Trusted Customer Zone
EvaluateDataQuality().process_rows(frame=PrivacyFilter_node1745260782345, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745260000407", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TrustedCustomerZone_node1745260793967 = glueContext.getSink(path="s3://udacity-lake1/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1745260793967")
TrustedCustomerZone_node1745260793967.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
TrustedCustomerZone_node1745260793967.setFormat("json")
TrustedCustomerZone_node1745260793967.writeFrame(PrivacyFilter_node1745260782345)
job.commit()
