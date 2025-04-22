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

# Script generated for node step_learner_landing
step_learner_landing_node1745300172029 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_learner_landing_node1745300172029")

# Script generated for node customer_curated
customer_curated_node1745300171189 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1745300171189")

# Script generated for node SQL Query
SqlQuery687 = '''
select c.*  from s join c on c.serialnumber =s.serialnumber
;
'''
SQLQuery_node1745301613972 = sparkSqlQuery(glueContext, query = SqlQuery687, mapping = {"s":step_learner_landing_node1745300172029, "c":customer_curated_node1745300171189}, transformation_ctx = "SQLQuery_node1745301613972")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745301613972, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745299521550", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745300235365 = glueContext.getSink(path="s3://udacity-lake1/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745300235365")
AmazonS3_node1745300235365.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_learner_trusted")
AmazonS3_node1745300235365.setFormat("json")
AmazonS3_node1745300235365.writeFrame(SQLQuery_node1745301613972)
job.commit()