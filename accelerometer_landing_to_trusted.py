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
AmazonS3_node1739211160063 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-saad/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1739211160063")

# Script generated for node Amazon S3
AmazonS3_node1739211128718 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-saad/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1739211128718")

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1739211358469 = Join.apply(frame1=AmazonS3_node1739211128718, frame2=AmazonS3_node1739211160063, keys1=["user"], keys2=["email"], transformation_ctx="CustomerPrivacyFilter_node1739211358469")

# Script generated for node Drop Fields
DropFields_node1739211477176 = DropFields.apply(frame=CustomerPrivacyFilter_node1739211358469, paths=["serialnumber", "birthday", "registrationdate", "sharewithresearchasofdate", "customername", "email", "lastupdatedate", "phone", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1739211477176")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1739211477176, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739211121845", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739211651344 = glueContext.getSink(path="s3://stedi-lake-saad/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739211651344")
AmazonS3_node1739211651344.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1739211651344.setFormat("json")
AmazonS3_node1739211651344.writeFrame(DropFields_node1739211477176)
job.commit()
