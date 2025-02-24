import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
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

# Script generated for node Amazon S3
AmazonS3_node1739261693031 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-stedi-lake-saad/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1739261693031")

# Script generated for node accelerometer_landing
accelerometer_landing_node1739261762112 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-saad/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1739261762112")

# Script generated for node Aggregate
Aggregate_node1739262953630 = sparkAggregate(glueContext, parentFrame = accelerometer_landing_node1739261762112, groups = ["user"], aggs = [["user", "count"]], transformation_ctx = "Aggregate_node1739262953630")

# Script generated for node Join
Join_node1739261809526 = Join.apply(frame1=AmazonS3_node1739261693031, frame2=Aggregate_node1739262953630, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1739261809526")

# Script generated for node Drop Fields
DropFields_node1739261830591 = DropFields.apply(frame=Join_node1739261809526, paths=["user"], transformation_ctx="DropFields_node1739261830591")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1739261830591, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739261100136", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739261848023 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1739261830591, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-saad/customer/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1739261848023")

job.commit()
