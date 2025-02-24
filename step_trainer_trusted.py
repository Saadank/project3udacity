import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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
AmazonS3_node1739263541930 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-saad/step-trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1739263541930")

# Script generated for node Amazon S3
AmazonS3_node1739263658117 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-saad/customer/curated/"], "recurse": True}, transformation_ctx="AmazonS3_node1739263658117")

# Script generated for node Join
Join_node1739263686502 = Join.apply(frame1=AmazonS3_node1739263658117, frame2=AmazonS3_node1739263541930, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1739263686502")

# Script generated for node Drop Fields
DropFields_node1739264775202 = DropFields.apply(frame=Join_node1739263686502, paths=["email", "`count(user)`", "phone", "birthday", "sharewithpublicasofdate", "sharewithresearchasofdate", "registrationdate", "customername", "sharewithfriendsasofdate", "lastupdatedate", "`.serialnumber`"], transformation_ctx="DropFields_node1739264775202")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1739264775202, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739263463300", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739263720789 = glueContext.getSink(path="s3://stedi-lake-saad/step-trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739263720789")
AmazonS3_node1739263720789.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1739263720789.setFormat("json")
AmazonS3_node1739263720789.writeFrame(DropFields_node1739264775202)
job.commit()
