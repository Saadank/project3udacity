
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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1739267570775 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-saad/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1739267570775")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1739267618255 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-saad/step-trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1739267618255")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT * 
FROM accelerometer_trusted 
JOIN step_trainer_trusted 
ON accelerometer_trusted.timestamp = step_trainer_trusted.sensorReadingTime;

'''
SQLQuery_node1739342233239 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":step_trainer_trusted_node1739267618255, "accelerometer_trusted":accelerometer_trusted_node1739267570775}, transformation_ctx = "SQLQuery_node1739342233239")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1739342233239, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739429368170", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739429567818 = glueContext.getSink(path="s3://stedi-lake-saad/ml-curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739429567818")
AmazonS3_node1739429567818.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1739429567818.setFormat("json")
AmazonS3_node1739429567818.writeFrame(SQLQuery_node1739342233239)
job.commit()
