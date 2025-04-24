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

# Script generated for node step_leaner_trusted
step_leaner_trusted_node1745505537615 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_learner_trusted", transformation_ctx="step_leaner_trusted_node1745505537615")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1745505536528 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1745505536528")

# Script generated for node SQL Query
SqlQuery607 = '''
select * 
from accelerometer a
join step_learner t on a.timestamp = t.sensorreadingtime;

'''
SQLQuery_node1745505630602 = sparkSqlQuery(glueContext, query = SqlQuery607, mapping = {"step_learner":step_leaner_trusted_node1745505537615, "accelerometer":accelerometer_trusted_node1745505536528}, transformation_ctx = "SQLQuery_node1745505630602")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745505630602, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745505531306", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745505740976 = glueContext.getSink(path="s3://udacity-lake1/step_trainer/machine_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745505740976")
AmazonS3_node1745505740976.setCatalogInfo(catalogDatabase="stedi",catalogTableName="maachine_curated")
AmazonS3_node1745505740976.setFormat("json")
AmazonS3_node1745505740976.writeFrame(SQLQuery_node1745505630602)
job.commit()