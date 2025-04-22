import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Customer Landing
CustomerLanding_node1745218107609 = glueContext.create_dynamic_frame.from_catalog(database="glue-databe", table_name="glue_table", transformation_ctx="CustomerLanding_node1745218107609")

# Script generated for node SQL Query
SqlQuery114 = '''
select * from myDataSource

'''
SQLQuery_node1745218131512 = sparkSqlQuery(glueContext, query = SqlQuery114, mapping = {"myDataSource":CustomerLanding_node1745218107609}, transformation_ctx = "SQLQuery_node1745218131512")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745218131512, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745218098947", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745218152104 = glueContext.getSink(path="s3://udacity-lake1", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745218152104")
AmazonS3_node1745218152104.setCatalogInfo(catalogDatabase="glue-databe",catalogTableName="glue_table")
AmazonS3_node1745218152104.setFormat("json")
AmazonS3_node1745218152104.writeFrame(SQLQuery_node1745218131512)
job.commit()